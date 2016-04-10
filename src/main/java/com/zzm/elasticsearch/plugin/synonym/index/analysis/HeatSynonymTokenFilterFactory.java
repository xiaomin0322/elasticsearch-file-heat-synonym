package com.zzm.elasticsearch.plugin.synonym.index.analysis;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisSettingsRequired;
import org.elasticsearch.index.analysis.HeatSynonymFilter;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.analysis.TokenizerFactoryFactory;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

@AnalysisSettingsRequired
public class HeatSynonymTokenFilterFactory extends AbstractTokenFilterFactory {
	private URL synonymFileURL;
	private static final ScheduledExecutorService threadPool = Executors
			.newScheduledThreadPool(1);
	private int interval;
	private SynonymMap synonymMap;
	private boolean ignoreCase;
	private boolean expand;
	private String format;
	private Analyzer analyzer;
	private String synonymsPath;
	private Environment env;

	private long lastModified;

	private final static List<HeatSynonymFilter> heatSynonymFilterAll = new ArrayList<HeatSynonymFilter>();

	/**
	 * 获取最后修改时间
	 * 
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public long getLastModifiedByUrl() {
		try {
			if (synonymsPath.startsWith("http")) {
				URLConnection urlConnection = synonymFileURL.openConnection();
				String LastModified = urlConnection
						.getHeaderField("Last-Modified");
				return lastModified = new Date(LastModified).getTime();
			} else {
				return lastModified = (new File(synonymFileURL.toURI()))
						.lastModified();
			}
		} catch (Exception e) {
			throw new ElasticsearchIllegalArgumentException(e.getMessage());
		}
	}

	/**
	 * 根据配置获取文件流
	 * 
	 * @return
	 */
	public Reader createReader() {

		try {
			if (synonymsPath.startsWith("http")) {
				synonymFileURL = new URL(synonymsPath);
			} else {
				synonymFileURL = env.resolveConfig(synonymsPath);
			}
			Reader rulesReader = new InputStreamReader(
					synonymFileURL.openStream(), Charsets.UTF_8);
			lastModified = getLastModifiedByUrl();
			return rulesReader;
		} catch (Exception e) {
			String message = String.format(Locale.ROOT,
					"IOException while reading synonyms_path: %s",
					e.getMessage());
			throw new ElasticsearchIllegalArgumentException(message);
		}
	}

	@Inject
	public HeatSynonymTokenFilterFactory(Index index,
			@IndexSettings Settings indexSettings, Environment env,
			IndicesAnalysisService indicesAnalysisService,
			Map<String, TokenizerFactoryFactory> tokenizerFactories,
			@Assisted String name, @Assisted Settings settings) {

		super(index, indexSettings, name, settings);

		this.ignoreCase = settings.getAsBoolean("ignore_case", false);
		this.expand = settings.getAsBoolean("expand", true);
		this.interval = settings.getAsInt("interval", 60);
		this.format = settings.get("format");
		this.synonymsPath = settings.get("synonyms_path", null);
		this.env = env;

		Reader rulesReader = null;
		if (settings.get("synonyms_path") != null) {
			rulesReader = createReader();
			lastModified = getLastModifiedByUrl();
		} else {
			throw new ElasticsearchIllegalArgumentException(
					"file watcher synonym requires `synonyms_path` to be configured");
		}

		String tokenizerName = settings.get("tokenizer", "whitespace");

		TokenizerFactoryFactory tokenizerFactoryFactory = tokenizerFactories
				.get(tokenizerName);
		if (tokenizerFactoryFactory == null) {
			tokenizerFactoryFactory = indicesAnalysisService
					.tokenizerFactoryFactory(tokenizerName);
		}
		if (tokenizerFactoryFactory == null) {
			throw new ElasticsearchIllegalArgumentException(
					"failed to find tokenizer [" + tokenizerName
							+ "] for synonym token filter");
		}
		final TokenizerFactory tokenizerFactory = tokenizerFactoryFactory
				.create(tokenizerName, settings);

		analyzer = new Analyzer() {
			@Override
			protected TokenStreamComponents createComponents(String fieldName,
					Reader reader) {
				Tokenizer tokenizer = tokenizerFactory == null ? new WhitespaceTokenizer(
						Lucene.ANALYZER_VERSION, reader) : tokenizerFactory
						.create(reader);
				TokenStream stream = ignoreCase ? new LowerCaseFilter(
						Lucene.ANALYZER_VERSION, tokenizer) : tokenizer;
				return new TokenStreamComponents(tokenizer, stream);
			}
		};

		createSynonymMap(rulesReader);

		threadPool.scheduleAtFixedRate(new FileMonitor(), 5, interval,
				TimeUnit.SECONDS);
	}

	public void createSynonymMap(Reader rulesReader) {
		try {
			SynonymMap.Builder parser = null;
			if ("wordnet".equalsIgnoreCase(format)) {
				parser = new WordnetSynonymParser(true, expand, analyzer);
				((WordnetSynonymParser) parser).parse(rulesReader);
			} else {
				parser = new SolrSynonymParser(true, expand, analyzer);
				((SolrSynonymParser) parser).parse(rulesReader);
			}
			synonymMap = parser.build();
		} catch (Exception e) {
			throw new ElasticsearchIllegalArgumentException(
					"failed to build synonyms", e);
		}
	}

	@Override
	public TokenStream create(TokenStream tokenStream) {
		// fst is null means no synonyms
		//System.out.println("HeatSynonymFilter create");
		HeatSynonymFilter heatSynonymFilter = new HeatSynonymFilter(
				tokenStream, synonymMap, ignoreCase);
		heatSynonymFilterAll.add(heatSynonymFilter);
		return synonymMap.fst == null ? tokenStream : heatSynonymFilter;
	}

	/**
	 * 监听文件修改
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean watchFile() throws Exception {
		boolean flag = false;
		if (lastModified < getLastModifiedByUrl()) {
			flag = true;
		}
		return flag;
	}

	public class FileMonitor implements Runnable {
		@Override
		public void run() {
			try {
				//System.out.println("FileMonitor start................................."+ lastModified);
				if (watchFile()) {
					Reader rulesReader = createReader();
					lastModified = getLastModifiedByUrl();
					createSynonymMap(rulesReader);
					for (HeatSynonymFilter filter : heatSynonymFilterAll) {
						filter.init(synonymMap, ignoreCase);
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("could not reload synonyms file: "
						+ e.getMessage());
			}
		}
	}
}