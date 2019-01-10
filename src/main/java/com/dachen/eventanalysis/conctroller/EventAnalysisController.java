package com.dachen.eventanalysis.conctroller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dachen.eventanalysis.service.ExcelExportService;
import com.dachen.eventanalysis.service.ImpalaDataService;
import com.dachen.util.JSONMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.*;

@RestController
@RequestMapping("/event")
public class EventAnalysisController {

    @Autowired
    private ImpalaDataService dataService;

    @Autowired
    private ExcelExportService excelService;

    @RequestMapping("/query")
    public JSONMessage query(@RequestParam(name = "event") String event,
                             @RequestParam(name = "index") String index,
                             @RequestParam(name = "dimension", required = false) String dimension,
                             @RequestParam(name = "filter_condition", required = false) String filter_condition,
                             @RequestParam(name = "dimension_date", required = false) String dimension_date,
                             @RequestParam(name = "begin_date", required = false) String begin_date,
                             @RequestParam(name = "end_date", required = false) String end_date
    ) throws Exception {
        Object res = dataService.eventAnalysis(event, index, dimension, filter_condition, dimension_date, begin_date, end_date);
        return JSONMessage.success("Request success", res);
    }

    @RequestMapping("/download")
    public void DownloadExcel(@RequestParam(name = "event") String event,
                              @RequestParam(name = "res") String res,
                              HttpServletResponse response) {

        String sheetName = event;
        List<String> fields = new LinkedList<>();
        List<String> firstCell = new LinkedList<>();
        List<String> secondCell = new LinkedList<>();

        JSONObject jo = JSONObject.parseObject(res);
        JSONObject data = jo.getJSONObject("data");
        JSONArray series = data.getJSONArray("series");

        firstCell = data.getJSONArray("x_axis").toJavaList(String.class);
        fields.add("时间");
        for (int i = 0; i < series.size(); i++) {
            String x = series.getJSONObject(i).getJSONArray("names").get(0).toString();
            String values = series.getJSONObject(i).getJSONArray("values").toString();
            fields.add(x);
            secondCell.add(values);
        }


        excelService.export(fields, sheetName, firstCell, secondCell, response);
    }

}
