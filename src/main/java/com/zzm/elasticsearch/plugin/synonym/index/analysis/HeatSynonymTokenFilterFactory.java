/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.zzm.elasticsearch.plugin.synonym.index.analysis;

import java.io.File;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.AnalysisSettingsRequired;
import org.elasticsearch.index.analysis.HeatSynonymFilter;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.analysis.TokenizerFactoryFactory;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

@AnalysisSettingsRequired
@SuppressWarnings("all")
public class HeatSynonymTokenFilterFactory extends AbstractTokenFilterFactory {

	private SynonymMap synonymMap;
	private boolean ignoreCase;

	private long lastModified;

	private HeatSynonymFilter mySynonymFilter;

	private List<HeatSynonymFilter> mySynonymFilterAll = new ArrayList<HeatSynonymFilter>();

	private ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

	private void initMonitor(final Index index, final Settings indexSettings,
			final Environment env,
			final IndicesAnalysisService indicesAnalysisService,
			final Map<String, TokenizerFactoryFactory> tokenizerFactories,
			final String name, final Settings settings) {
		int interval = settings.getAsInt("interval", 60);
		// System.out.println("interval=================="+interval);
		pool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					if (mySynonymFilter != null) {
						// System.out.println("init SynonymTokenFilterFactory................ ");
						boolean flag = init(index, indexSettings, env,
								indicesAnalysisService, tokenizerFactories,
								name, settings);
						if (flag) {
							// System.out.println("mySynonymFilterAll size is "+mySynonymFilterAll.size());
							for (HeatSynonymFilter heatSynonymFilter : mySynonymFilterAll) {
								heatSynonymFilter.init(synonymMap, ignoreCase);
							}
							// mySynonymFilter.init(synonymMap, ignoreCase);
						} else {
							// System.out.println("文件没有修改。。。。。。。。。。。。。。。。。。");
						}

					} else {
						// System.out.println("mySynonymFilter is null.................");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}, 10, interval, TimeUnit.SECONDS);
	}

	@Inject
	public HeatSynonymTokenFilterFactory(Index index,
			@IndexSettings Settings indexSettings, Environment env,
			IndicesAnalysisService indicesAnalysisService,
			Map<String, TokenizerFactoryFactory> tokenizerFactories,
			@Assisted String name, @Assisted Settings settings) {
		super(index, indexSettings, name, settings);
		// System.out.println("SynonymTokenFilterFactory init <<<<<<<<<<<<<<<<<<<<<");
		boolean flag = init(index, indexSettings, env, indicesAnalysisService,
				tokenizerFactories, name, settings);
		// System.out.println("SynonymTokenFilterFactory init >>>>>>>>>>>>>>>>>>>>>>"+
		// flag);
		if (flag) {
			initMonitor(index, indexSettings, env, indicesAnalysisService,
					tokenizerFactories, name, settings);
		}
	}

	public boolean init(Index index, Settings indexSettings, Environment env,
			IndicesAnalysisService indicesAnalysisService,
			Map<String, TokenizerFactoryFactory> tokenizerFactories,
			String name, Settings settings) {

		boolean flag = true;
		Reader rulesReader = null;
		if (settings.getAsArray("synonyms", null) != null) {
			List<String> rules = Analysis
					.getWordList(env, settings, "synonyms");
			StringBuilder sb = new StringBuilder();
			for (String line : rules) {
				sb.append(line).append(System.getProperty("line.separator"));
			}
			rulesReader = new FastStringReader(sb.toString());
		} else if (settings.get("synonyms_path") != null) {
			String filePath = settings.get("synonyms_path");
			// System.out.println("synonyms_path :" + filePath);
			URL fileUrl = env.resolveConfig(filePath);
			File file = null;
			try {
				file = new File(fileUrl.toURI());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			if (file != null && file.exists()) {
				if (lastModified != file.lastModified()) {
					lastModified = file.lastModified();
				} else {
					return false;
				}
			}
			rulesReader = Analysis.getReaderFromFile(env, settings,
					"synonyms_path");
		} else {
			throw new ElasticsearchIllegalArgumentException(
					"synonym requires either `synonyms` or `synonyms_path` to be configured");
		}

		this.ignoreCase = settings.getAsBoolean("ignore_case", false);
		boolean expand = settings.getAsBoolean("expand", true);

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

		Analyzer analyzer = new Analyzer() {
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

		try {
			SynonymMap.Builder parser = null;

			if ("wordnet".equalsIgnoreCase(settings.get("format"))) {
				parser = new WordnetSynonymParser(true, expand, analyzer);
				((WordnetSynonymParser) parser).parse(rulesReader);
			} else {
				parser = new SolrSynonymParser(true, expand, analyzer);
				((SolrSynonymParser) parser).parse(rulesReader);
			}
			synonymMap = parser.build();
			// System.out.println("synonymMap.words.size=="+
			// synonymMap.words.size());
		} catch (Exception e) {
			throw new ElasticsearchIllegalArgumentException(
					"failed to build synonyms", e);
		}
		return flag;
	}

	@Override
	@SuppressWarnings("resource")
	public TokenStream create(TokenStream tokenStream) {
		// System.out.println("create.............SynonymTokenFilterFactory");
		mySynonymFilter = new HeatSynonymFilter(tokenStream, synonymMap,
				ignoreCase);
		mySynonymFilterAll.add(mySynonymFilter);
		// fst is null means no synonyms
		return synonymMap.fst == null ? tokenStream : mySynonymFilter;
	}

}
