package com.dachen.eventanalysis.service;

import com.dachen.eventanalysis.dataprovider.ExcelExportProvider;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

@Service
public class ExcelExportService {

    @Autowired
    ExcelExportProvider excelProvider = new ExcelExportProvider();

    public boolean export(List<String> feilds, String sheetName, List<String> firstCell, List<String> secondCell, HttpServletResponse response) {
        try {
            HSSFWorkbook wb = excelProvider.generateExcel();
            wb = excelProvider.generateSheet(wb, sheetName, feilds, firstCell, secondCell);

            excelProvider.export(wb, response);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
