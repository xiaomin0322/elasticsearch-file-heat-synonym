package com.zzm.elasticsearch.plugin;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.plugins.AbstractPlugin;

import com.zzm.elasticsearch.plugin.synonym.index.analysis.HeatSynonymTokenFilterFactory;

public class HeatSynonymPlugin extends AbstractPlugin {  
	   
    public String name() {  
      return "elasticsearch-file-heat-synonym";  
    }  
    
    public String description() {
		return "Analysis-plugin for synonym";
	}
    
    @Override public void processModule(Module module) {  
      if (module instanceof AnalysisModule) {  
        ((AnalysisModule) module).addTokenFilter("file_heat_synonym", HeatSynonymTokenFilterFactory.class);  
      }  
    }  
  } 