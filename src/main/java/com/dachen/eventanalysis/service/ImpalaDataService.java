package com.dachen.eventanalysis.service;

import com.dachen.eventanalysis.dataprovider.AnalysisCommonUtils;
import com.dachen.eventanalysis.dataprovider.EventAnalysisProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ImpalaDataService {


    @Autowired
    EventAnalysisProvider eventProvider;

    public Object eventAnalysis(String event, String index, String dimension,
                                String filter_condition, String dimension_date, String begin_date, String end_date) throws Exception {
        Object eventMap = eventProvider.eventAnalysis(event, index, dimension, filter_condition, dimension_date, begin_date, end_date);
        return eventMap;
    }


}
