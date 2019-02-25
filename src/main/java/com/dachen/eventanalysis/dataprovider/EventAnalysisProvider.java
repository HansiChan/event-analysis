package com.dachen.eventanalysis.dataprovider;

import com.dachen.eventanalysis.pojo.AnalysisLineChartVo;
import com.dachen.eventanalysis.pojo.AnalysisListVo;
import com.dachen.eventanalysis.pojo.AnalysisVo;
import com.dachen.util.ImpalaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Repository
public class EventAnalysisProvider {

    Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ImpalaUtil impalaUtil;
    @Autowired
    AnalysisCommonUtils analysisCommonUtils;


    public Object eventAnalysis(String event, String index, String dimension, String filter_condition,
                                String dimension_date, String begin_date, String end_date) throws Exception {

        int daysLen = AnalysisCommonUtils.getDayLength(begin_date, end_date);
        int subLength = 1;

        if ("hour".equals(dimension_date)) {
            end_date = begin_date;
            daysLen = 24;
        }

        String sqlFilter = "";
        String sql = "";
        String sqlIndex = "";
        String[] subList = {event};
        String dateSql = dimension_date + "s";
        String dimensionFilter ="if(" + dimension + " is null or " + dimension + "='' or " + dimension + " in ('NULL','未知'),\"其他\"," + dimension + ")";
        if("ifcard".equals(dimension)){dimensionFilter="if(" + dimension + " is null or " + dimension + "='' or " + dimension + " in ('NULL','未知'),\"无\"," + dimension + ")";}
        String moduleFilter = " t.module= '" + event +"' and ";
        String timeZone = " days >='" + begin_date + "' and days <='" + end_date + "' ";
        if("全部事件".equals(event)){
            moduleFilter ="";
        }
        if (!"".equals(filter_condition) && filter_condition != null) {
            sqlFilter = sqlFilter + filter_condition.replace("where", "and");
            if(filter_condition.contains("其他")){
                String x =filter_condition.split(" ")[1];
                sqlFilter = filter_condition.replace("where", "and").replace("'其他'","'','NULL','未知'") + " or " + x + " is null  and" + timeZone;
            } else if(filter_condition.contains("无")){
                String x =filter_condition.split(" ")[1];
                sqlFilter = filter_condition.replace("where", "and") + " or " + x + " is null  and" + timeZone;
            }
        }

        if (null != dimension && !"".equals(dimension)) {
            subLength = analysisCommonUtils.filter(dimension).size();
            subList = analysisCommonUtils.filter(dimension).toArray(new String[analysisCommonUtils.filter(dimension).size()]);
        } else {
            dimensionFilter = "'" + event + "'";
        }



        if ("people".equals(index)) {
            sqlIndex = "count(distinct(userid))";
        } else if ("times".equals(index)) {
            sqlIndex = "count(*)";
        } else if ("avgtimes".equals(index)) {
            sqlIndex = "count(*)/count(distinct(userid))";
        }

        if ("active".equals(index)) {
            String tableA = "select " + dateSql + " as dt," + dimensionFilter + " as name,"
                    + "count(distinct(userid)) as value from dw.dw_user_event_r as t where " + moduleFilter
                    + timeZone + sqlFilter + " group by dt,name order by value desc";

            String tableB = "select " + dateSql + " as dt," + dimensionFilter + " as name,"
                    + "count(distinct(userid)) as value from dw.dw_user_login_r where" + timeZone
                    + sqlFilter + " group by dt,name order by value desc";
            sql = "select a.dt,a.name,a.value/b.value as value from (" + tableA + ") as a join (" + tableB + ") as b on a.dt=b.dt and a.name=b.name "
                    + "group by dt,name,a.value,b.value order by value desc";
        } else {
            sql = "with t as (select * from dw.dw_user_event_r) select " + dateSql + " as dt," + dimensionFilter + " as name,"
                    + sqlIndex + " as value from t where " + moduleFilter
                    + timeZone + sqlFilter + " group by dt,name order by value desc";
        }

        List<AnalysisVo> voList = new ArrayList<>();
        List<Map> dtNameList = new ArrayList<>();
        Set<String> dtSet = new HashSet<>();

        Connection conn = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            LOG.info(sql);
            conn = impalaUtil.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                dtSet.add(rs.getString(1));
                map.put(rs.getString(1), rs.getString(2).trim());
                dtNameList.add(map);

                AnalysisVo vo = new AnalysisVo();
                vo.setDt(rs.getString(1));
                vo.setName(rs.getString(2).trim());
                if ("avgtimes".equals(index)) {
                    vo.setValue(AnalysisCommonUtils.formatNumber(rs.getFloat(3),2));
                } else if ("active".equals(index)) {
                    vo.setValue(AnalysisCommonUtils.formatNumber(rs.getFloat(3),4));
                } else {
                    vo.setValue(rs.getString(3));
                }
                voList.add(vo);
            }
        } catch (Exception e) {
            throw new Exception("ERROR:" + e.getMessage(), e);
        } finally {
            try {
                conn.close();
                stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        Map<String, List> dt2Name = AnalysisCommonUtils.mapCombine(dtNameList);
        if (dimension != null && !"".equals(dimension)) {
            if (sqlFilter.contains(dimension)) {
                List nameList = new LinkedList();
                for (Map.Entry<String, List> entry : dt2Name.entrySet()) {
                    for (Object value : entry.getValue()) {
                        if (!nameList.contains(value)) {
                            nameList.add(value);
                        }
                    }
                    subList = (String[]) nameList.toArray(new String[nameList.size()]);
                    subLength = subList.length;
                /*if (entry.getValue().size() > nameListSize) {
                    nameListSize = entry.getValue().size();
                    subString = (String[]) entry.getValue().toArray(new String[entry.getValue().size()]);
                    subLength = subString.length;
                }*/
                }
            }
        }
        for (Map.Entry<String, List> entry : dt2Name.entrySet()) {
            if (entry.getValue().size() <= subLength) {
                List<String> nameList = new ArrayList<>();
                if (dimension != null && !"".equals(dimension)) {
                    for (String sub : subList) {
                        if (!entry.getValue().contains(sub.trim())) {
                            nameList.add(sub.trim());
                        }
                    }
                }
                for (String xx : nameList) {
                    AnalysisVo vo = new AnalysisVo();
                    vo.setDt(entry.getKey());
                    vo.setName(xx.trim());
                    if ("avgtimes".equals(index)) {
                        vo.setValue("0.00");
                    } else if ("active".equals(index)) {
                        vo.setValue("0.0000");
                    } else {
                        vo.setValue("0");
                    }
                    voList.add(vo);
                }
            }
        }

        List<String> fullDateList = AnalysisCommonUtils.dateSplit(begin_date, end_date, dateSql);
        if (voList.size() <= daysLen * subLength) {
            for (String day : fullDateList) {
                if (!dtSet.contains(day)) {
                    for (String sub : subList) {
                        AnalysisVo vo2 = new AnalysisVo();
                        vo2.setDt(day);
                        if (null == dimension && "".equals(dimension)) {
                            vo2.setName(event);
                        } else {
                            vo2.setName(sub.trim());
                        }
                        if ("avgtimes".equals(index)) {
                            vo2.setValue("0.00");
                        } else if ("active".equals(index)) {
                            vo2.setValue("0.0000");
                        } else {
                            vo2.setValue("0");
                        }
                        voList.add(vo2);
                    }
                }
            }
        }

        voList = AnalysisCommonUtils.sortList(voList, "dt", false);
        List<Map> aggMap = new ArrayList<>();
        List<String> xList = new ArrayList<>();
        for (AnalysisVo vo : voList) {
            Map<String, String> map = new HashMap<>();
            xList.add(vo.getDt());
            map.put(vo.getName(), vo.getValue());
            aggMap.add(map);
        }
        xList = AnalysisCommonUtils.removeDuplicate(xList);

        Map<String, List> map = AnalysisCommonUtils.mapCombine(aggMap);
        List<AnalysisListVo> dvoList = new ArrayList<>();
        for (Map.Entry<String, List> entry : map.entrySet()) {
            AnalysisListVo dvo = new AnalysisListVo();
            List<String> nameList = new LinkedList<>();
            List<String> valueList = new LinkedList<>();
            nameList.add(entry.getKey());
            for (int i = 0; i < entry.getValue().size(); i++) {
                valueList.add((String) entry.getValue().get(i));
            }
            dvo.setNames(nameList);
            dvo.setValues(valueList);
            dvoList.add(dvo);
        }

        AnalysisCommonUtils.sortValue(dvoList);
        AnalysisLineChartVo dyo = new AnalysisLineChartVo();
        dyo.setSeries(dvoList);
        dyo.setX_axis(xList);

        return dyo;
    }

}
