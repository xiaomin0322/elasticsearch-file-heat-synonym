package org.elasticsearch.index.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.FST;

public class HeatSynonymFilter extends TokenFilter {

	public static final String TYPE_SYNONYM = "SYNONYM";

	private SynonymMap synonyms;

	private boolean ignoreCase;
	private int rollBufferSize;

	private int captureCount;

	// TODO: we should set PositionLengthAttr too...

	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
	private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
	private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
	private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
	private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

	// How many future input tokens have already been matched
	// to a synonym; because the matching is "greedy" we don't
	// try to do any more matching for such tokens:
	private int inputSkipCount;

	// Hold all buffered (read ahead) stacked input tokens for
	// a future position. When multiple tokens are at the
	// same position, we only store (and match against) the
	// term for the first token at the position, but capture
	// state for (and enumerate) all other tokens at this
	// position:
	private static class PendingInput {
		final CharsRef term = new CharsRef();
		AttributeSource.State state;
		boolean keepOrig;
		boolean matched;
		boolean consumed = true;
		int startOffset;
		int endOffset;

		public void reset() {
			state = null;
			consumed = true;
			keepOrig = false;
			matched = false;
		}
	};

	// Rolling buffer, holding pending input tokens we had to
	// clone because we needed to look ahead, indexed by
	// position:
	private  PendingInput[] futureInputs;

	// Holds pending output synonyms for one future position:
	private static class PendingOutputs {
		CharsRef[] outputs;
		int[] endOffsets;
		int[] posLengths;
		int upto;
		int count;
		int posIncr = 1;
		int lastEndOffset;
		int lastPosLength;

		public PendingOutputs() {
			outputs = new CharsRef[1];
			endOffsets = new int[1];
			posLengths = new int[1];
		}

		public void reset() {
			upto = count = 0;
			posIncr = 1;
		}

		public CharsRef pullNext() {
			assert upto < count;
			lastEndOffset = endOffsets[upto];
			lastPosLength = posLengths[upto];
			final CharsRef result = outputs[upto++];
			posIncr = 0;
			if (upto == count) {
				reset();
			}
			return result;
		}

		public int getLastEndOffset() {
			return lastEndOffset;
		}

		public int getLastPosLength() {
			return lastPosLength;
		}

		public void add(char[] output, int offset, int len, int endOffset,
				int posLength) {
			if (count == outputs.length) {
				final CharsRef[] next = new CharsRef[ArrayUtil.oversize(
						1 + count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
				System.arraycopy(outputs, 0, next, 0, count);
				outputs = next;
			}
			if (count == endOffsets.length) {
				final int[] next = new int[ArrayUtil.oversize(1 + count,
						RamUsageEstimator.NUM_BYTES_INT)];
				System.arraycopy(endOffsets, 0, next, 0, count);
				endOffsets = next;
			}
			if (count == posLengths.length) {
				final int[] next = new int[ArrayUtil.oversize(1 + count,
						RamUsageEstimator.NUM_BYTES_INT)];
				System.arraycopy(posLengths, 0, next, 0, count);
				posLengths = next;
			}
			if (outputs[count] == null) {
				outputs[count] = new CharsRef();
			}
			outputs[count].copyChars(output, offset, len);
			// endOffset can be -1, in which case we should simply
			// use the endOffset of the input token, or X >= 0, in
			// which case we use X as the endOffset for this output
			endOffsets[count] = endOffset;
			posLengths[count] = posLength;
			count++;
		}
	};

	private final ByteArrayDataInput bytesReader = new ByteArrayDataInput();

	// Rolling buffer, holding stack of pending synonym
	// outputs, indexed by position:
	private  PendingOutputs[] futureOutputs;

	// Where (in rolling buffers) to write next input saved state:
	private int nextWrite;

	// Where (in rolling buffers) to read next input saved state:
	private int nextRead;

	// True once we've read last token
	private boolean finished;

	private FST.Arc<BytesRef> scratchArc;

	private FST<BytesRef> fst;

	private FST.BytesReader fstReader;

	private BytesRef scratchBytes = new BytesRef();
	private CharsRef scratchChars = new CharsRef();

	/**
	 * @param input
	 *            input tokenstream
	 * @param synonyms
	 *            synonym map
	 * @param ignoreCase
	 *            case-folds input for matching with
	 *            {@link Character#toLowerCase(int)}. Note, if you set this to
	 *            true, its your responsibility to lowercase the input entries
	 *            when you create the {@link SynonymMap}
	 */
	public HeatSynonymFilter(TokenStream input, SynonymMap synonyms,
			boolean ignoreCase) {
		super(input);
       init(synonyms, ignoreCase);
	}

	public void init(SynonymMap synonyms, boolean ignoreCase) {
		this.synonyms = synonyms;
		this.ignoreCase = ignoreCase;
		this.fst = synonyms.fst;
		this.fstReader = fst.getBytesReader();
		if (fst == null) {
			throw new IllegalArgumentException("fst must be non-null");
		}

		// Must be 1+ so that when roll buffer is at full
		// lookahead we can distinguish this full buffer from
		// the empty buffer:
		rollBufferSize = 1 + synonyms.maxHorizontalContext;

		futureInputs = new PendingInput[rollBufferSize];
		futureOutputs = new PendingOutputs[rollBufferSize];
		for (int pos = 0; pos < rollBufferSize; pos++) {
			futureInputs[pos] = new PendingInput();
			futureOutputs[pos] = new PendingOutputs();
		}

		// System.out.println("FSTFilt maxH=" + synonyms.maxHorizontalContext);

		scratchArc = new FST.Arc<BytesRef>();
	}

	private void capture() {
		captureCount++;
		// System.out.println("  capture slot=" + nextWrite);
		final PendingInput input = futureInputs[nextWrite];

		input.state = captureState();
		input.consumed = false;
		input.term.copyChars(termAtt.buffer(), 0, termAtt.length());

		nextWrite = rollIncr(nextWrite);

		// Buffer head should never catch up to tail:
		assert nextWrite != nextRead;
	}

	/*
	 * This is the core of this TokenFilter: it locates the synonym matches and
	 * buffers up the results into futureInputs/Outputs.
	 * 
	 * NOTE: this calls input.incrementToken and does not capture the state if
	 * no further tokens were checked. So caller must then forward state to our
	 * caller, or capture:
	 */
	private int lastStartOffset;
	private int lastEndOffset;

	private void parse() throws IOException {
		// System.out.println("\nS: parse");

		assert inputSkipCount == 0;

		int curNextRead = nextRead;

		// Holds the longest match we've seen so far:
		BytesRef matchOutput = null;
		int matchInputLength = 0;
		int matchEndOffset = -1;

		BytesRef pendingOutput = fst.outputs.getNoOutput();
		fst.getFirstArc(scratchArc);

		assert scratchArc.output == fst.outputs.getNoOutput();

		int tokenCount = 0;

		byToken: while (true) {

			// Pull next token's chars:
			final char[] buffer;
			final int bufferLen;
			// System.out.println("  cycle nextRead=" + curNextRead +
			// " nextWrite=" + nextWrite);

			int inputEndOffset = 0;

			if (curNextRead == nextWrite) {

				// We used up our lookahead buffer of input tokens
				// -- pull next real input token:

				if (finished) {
					break;
				} else {
					// System.out.println("  input.incrToken");
					assert futureInputs[nextWrite].consumed;
					// Not correct: a syn match whose output is longer
					// than its input can set future inputs keepOrig
					// to true:
					// assert !futureInputs[nextWrite].keepOrig;
					if (input.incrementToken()) {
						buffer = termAtt.buffer();
						bufferLen = termAtt.length();
						final PendingInput input = futureInputs[nextWrite];
						lastStartOffset = input.startOffset = offsetAtt
								.startOffset();
						lastEndOffset = input.endOffset = offsetAtt.endOffset();
						inputEndOffset = input.endOffset;
						// System.out.println("  new token=" + new
						// String(buffer, 0, bufferLen));
						if (nextRead != nextWrite) {
							capture();
						} else {
							input.consumed = false;
						}

					} else {
						// No more input tokens
						// System.out.println("      set end");
						finished = true;
						break;
					}
				}
			} else {
				// Still in our lookahead
				buffer = futureInputs[curNextRead].term.chars;
				bufferLen = futureInputs[curNextRead].term.length;
				inputEndOffset = futureInputs[curNextRead].endOffset;
				// System.out.println("  old token=" + new String(buffer, 0,
				// bufferLen));
			}

			tokenCount++;

			// Run each char in this token through the FST:
			int bufUpto = 0;
			while (bufUpto < bufferLen) {
				final int codePoint = Character.codePointAt(buffer, bufUpto,
						bufferLen);
				if (fst.findTargetArc(
						ignoreCase ? Character.toLowerCase(codePoint)
								: codePoint, scratchArc, scratchArc, fstReader) == null) {
					// System.out.println("    stop");
					break byToken;
				}

				// Accum the output
				pendingOutput = fst.outputs.add(pendingOutput,
						scratchArc.output);
				// System.out.println("    char=" + buffer[bufUpto] + " output="
				// + pendingOutput + " arc.output=" + scratchArc.output);
				bufUpto += Character.charCount(codePoint);
			}

			// OK, entire token matched; now see if this is a final
			// state:
			if (scratchArc.isFinal()) {
				matchOutput = fst.outputs.add(pendingOutput,
						scratchArc.nextFinalOutput);
				matchInputLength = tokenCount;
				matchEndOffset = inputEndOffset;
				// System.out.println("  found matchLength=" + matchInputLength
				// + " output=" + matchOutput);
			}

			// See if the FST wants to continue matching (ie, needs to
			// see the next input token):
			if (fst.findTargetArc(SynonymMap.WORD_SEPARATOR, scratchArc,
					scratchArc, fstReader) == null) {
				// No further rules can match here; we're done
				// searching for matching rules starting at the
				// current input position.
				break;
			} else {
				// More matching is possible -- accum the output (if
				// any) of the WORD_SEP arc:
				pendingOutput = fst.outputs.add(pendingOutput,
						scratchArc.output);
				if (nextRead == nextWrite) {
					capture();
				}
			}

			curNextRead = rollIncr(curNextRead);
		}

		if (nextRead == nextWrite && !finished) {
			// System.out.println("  skip write slot=" + nextWrite);
			nextWrite = rollIncr(nextWrite);
		}

		if (matchOutput != null) {
			// System.out.println("  add matchLength=" + matchInputLength +
			// " output=" + matchOutput);
			inputSkipCount = matchInputLength;
			addOutput(matchOutput, matchInputLength, matchEndOffset);
		} else if (nextRead != nextWrite) {
			// Even though we had no match here, we set to 1
			// because we need to skip current input token before
			// trying to match again:
			inputSkipCount = 1;
		} else {
			assert finished;
		}

		// System.out.println("  parse done inputSkipCount=" + inputSkipCount +
		// " nextRead=" + nextRead + " nextWrite=" + nextWrite);
	}

	// Interleaves all output tokens onto the futureOutputs:
	private void addOutput(BytesRef bytes, int matchInputLength,
			int matchEndOffset) {
		bytesReader.reset(bytes.bytes, bytes.offset, bytes.length);

		final int code = bytesReader.readVInt();
		final boolean keepOrig = (code & 0x1) == 0;
		final int count = code >>> 1;
		// System.out.println("  addOutput count=" + count + " keepOrig=" +
		// keepOrig);
		for (int outputIDX = 0; outputIDX < count; outputIDX++) {
			synonyms.words.get(bytesReader.readVInt(), scratchBytes);
			// System.out.println("    outIDX=" + outputIDX + " bytes=" +
			// scratchBytes.length);
			UnicodeUtil.UTF8toUTF16(scratchBytes, scratchChars);
			int lastStart = scratchChars.offset;
			final int chEnd = lastStart + scratchChars.length;
			int outputUpto = nextRead;
			for (int chIDX = lastStart; chIDX <= chEnd; chIDX++) {
				if (chIDX == chEnd
						|| scratchChars.chars[chIDX] == SynonymMap.WORD_SEPARATOR) {
					final int outputLen = chIDX - lastStart;
					// Caller is not allowed to have empty string in
					// the output:
					assert outputLen > 0 : "output contains empty string: "
							+ scratchChars;
					final int endOffset;
					final int posLen;
					if (chIDX == chEnd && lastStart == scratchChars.offset) {
						// This rule had a single output token, so, we set
						// this output's endOffset to the current
						// endOffset (ie, endOffset of the last input
						// token it matched):
						endOffset = matchEndOffset;
						posLen = keepOrig ? matchInputLength : 1;
					} else {
						// This rule has more than one output token; we
						// can't pick any particular endOffset for this
						// case, so, we inherit the endOffset for the
						// input token which this output overlaps:
						endOffset = -1;
						posLen = 1;
					}
					futureOutputs[outputUpto].add(scratchChars.chars,
							lastStart, outputLen, endOffset, posLen);
					// System.out.println("      " + new
					// String(scratchChars.chars, lastStart, outputLen) +
					// " outputUpto=" + outputUpto);
					lastStart = 1 + chIDX;
					// System.out.println("  slot=" + outputUpto + " keepOrig="
					// + keepOrig);
					outputUpto = rollIncr(outputUpto);
					assert futureOutputs[outputUpto].posIncr == 1 : "outputUpto="
							+ outputUpto + " vs nextWrite=" + nextWrite;
				}
			}
		}

		int upto = nextRead;
		for (int idx = 0; idx < matchInputLength; idx++) {
			futureInputs[upto].keepOrig |= keepOrig;
			futureInputs[upto].matched = true;
			upto = rollIncr(upto);
		}
	}

	// ++ mod rollBufferSize
	private int rollIncr(int count) {
		count++;
		if (count == rollBufferSize) {
			return 0;
		} else {
			return count;
		}
	}

	// for testing
	int getCaptureCount() {
		return captureCount;
	}

	@Override
	public boolean incrementToken() throws IOException {

		// System.out.println("\nS: incrToken inputSkipCount=" + inputSkipCount
		// + " nextRead=" + nextRead + " nextWrite=" + nextWrite);

		while (true) {

			// First play back any buffered future inputs/outputs
			// w/o running parsing again:
			while (inputSkipCount != 0) {

				// At each position, we first output the original
				// token

				// TODO: maybe just a PendingState class, holding
				// both input & outputs?
				final PendingInput input = futureInputs[nextRead];
				final PendingOutputs outputs = futureOutputs[nextRead];

				// System.out.println("  cycle nextRead=" + nextRead +
				// " nextWrite=" + nextWrite + " inputSkipCount="+
				// inputSkipCount + " input.keepOrig=" + input.keepOrig +
				// " input.consumed=" + input.consumed + " input.state=" +
				// input.state);

				if (!input.consumed && (input.keepOrig || !input.matched)) {
					if (input.state != null) {
						// Return a previously saved token (because we
						// had to lookahead):
						restoreState(input.state);
					} else {
						// Pass-through case: return token we just pulled
						// but didn't capture:
						assert inputSkipCount == 1 : "inputSkipCount="
								+ inputSkipCount + " nextRead=" + nextRead;
					}
					input.reset();
					if (outputs.count > 0) {
						outputs.posIncr = 0;
					} else {
						nextRead = rollIncr(nextRead);
						inputSkipCount--;
					}
					// System.out.println("  return token=" +
					// termAtt.toString());
					return true;
				} else if (outputs.upto < outputs.count) {
					// Still have pending outputs to replay at this
					// position
					input.reset();
					final int posIncr = outputs.posIncr;
					final CharsRef output = outputs.pullNext();
					clearAttributes();
					termAtt.copyBuffer(output.chars, output.offset,
							output.length);
					typeAtt.setType(TYPE_SYNONYM);
					int endOffset = outputs.getLastEndOffset();
					if (endOffset == -1) {
						endOffset = input.endOffset;
					}
					offsetAtt.setOffset(input.startOffset, endOffset);
					posIncrAtt.setPositionIncrement(posIncr);
					posLenAtt.setPositionLength(outputs.getLastPosLength());
					if (outputs.count == 0) {
						// Done with the buffered input and all outputs at
						// this position
						nextRead = rollIncr(nextRead);
						inputSkipCount--;
					}
					// System.out.println("  return token=" +
					// termAtt.toString());
					return true;
				} else {
					// Done with the buffered input and all outputs at
					// this position
					input.reset();
					nextRead = rollIncr(nextRead);
					inputSkipCount--;
				}
			}

			if (finished && nextRead == nextWrite) {
				// End case: if any output syns went beyond end of
				// input stream, enumerate them now:
				final PendingOutputs outputs = futureOutputs[nextRead];
				if (outputs.upto < outputs.count) {
					final int posIncr = outputs.posIncr;
					final CharsRef output = outputs.pullNext();
					futureInputs[nextRead].reset();
					if (outputs.count == 0) {
						nextWrite = nextRead = rollIncr(nextRead);
					}
					clearAttributes();
					// Keep offset from last input token:
					offsetAtt.setOffset(lastStartOffset, lastEndOffset);
					termAtt.copyBuffer(output.chars, output.offset,
							output.length);
					typeAtt.setType(TYPE_SYNONYM);
					// System.out.println("  set posIncr=" + outputs.posIncr +
					// " outputs=" + outputs);
					posIncrAtt.setPositionIncrement(posIncr);
					// System.out.println("  return token=" +
					// termAtt.toString());
					return true;
				} else {
					return false;
				}
			}

			// Find new synonym matches:
			parse();
		}
	}

	@Override
	public void reset() throws IOException {
		super.reset();
		captureCount = 0;
		finished = false;
		inputSkipCount = 0;
		nextRead = nextWrite = 0;

		// In normal usage these resets would not be needed,
		// since they reset-as-they-are-consumed, but the app
		// may not consume all input tokens (or we might hit an
		// exception), in which case we have leftover state
		// here:
		for (PendingInput input : futureInputs) {
			input.reset();
		}
		for (PendingOutputs output : futureOutputs) {
			output.reset();
		}
	}
}
