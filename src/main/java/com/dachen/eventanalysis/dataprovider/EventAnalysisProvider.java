package com.dachen.eventanalysis.dataprovider;

import com.dachen.eventanalysis.pojo.AnalysisLineChartVo;
import com.dachen.eventanalysis.pojo.AnalysisListVo;
import com.dachen.eventanalysis.pojo.AnalysisVo;
import com.dachen.util.ImpalaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.dachen.util.ImpalaUtil.getConnection;


@Repository
public class EventAnalysisProvider {

    Logger LOG = LoggerFactory.getLogger(this.getClass());

    Connection conn = null;
    Statement stat = null;
    ResultSet rs = null;

//    @Autowired
//    ImpalaUtil conn;

    public List<String> getEvents() throws Exception {
        String sql = "select distinct module from dw.dw_full_point where module is not null and trim(module)<>'' and module<>='null' ";
        List<String> event = new LinkedList<>();
        try (Connection connection = getConnection();
             Statement stat = connection.createStatement();
             ResultSet rs = stat.executeQuery(sql)) {
            while (rs.next()) {
                event.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new Exception("ERROR:" + e.getMessage(), e);
        }
        return event;
    }

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
        String dimensionFilter ="if(" + dimension + " is null,\"未知\"," + dimension + ")";
        String tableJoin = "select * from dw.dw_full_point";

        if (!"".equals(filter_condition) && filter_condition != null) {
            sqlFilter = sqlFilter + filter_condition.replace("where", "and (") + ") ";
        }

        if (null != dimension && !"".equals(dimension)) {
            subLength = AnalysisCommonUtils.filter(dimension).toString().split(",").length;
            subList = AnalysisCommonUtils.filter(dimension).toString().replace("[", "")
                    .replace("]", "").replace(" ", "").split(",");
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
                    + "count(distinct(userid)) as value from dw.dw_full_point as t where t.module = '" + event
                    + "' and days >='" + begin_date + "' and days <='" + end_date + "' " + sqlFilter + " group by dt,name order by value desc";

            String tableB = "select " + dateSql + " as dt," + dimensionFilter + " as name,"
                    + "count(distinct(userid)) as value from dw.dw_user_login where days >='" + begin_date + "' and days <='"
                    + end_date + "' " + sqlFilter + " group by dt,name order by value desc";
            sql = "select a.dt,a.name,a.value/b.value as value from (" + tableA + ") as a join (" + tableB + ") as b on a.dt=b.dt and a.name=b.name "
                    + "group by dt,name,a.value,b.value order by value desc";
        } else {
            sql = "with t as (" + tableJoin + ") select " + dateSql + " as dt," + dimensionFilter + " as name,"
                    + sqlIndex + " as value from t where t.module = '" + event
                    + "' and days >='" + begin_date + "' and days <='" + end_date + "' " + sqlFilter + " group by dt,name order by value desc";
        }

        List<AnalysisVo> voList = new ArrayList<>();
        List<Map> dtNameList = new ArrayList<>();
        Set<String> dtSet = new HashSet<>();

        try {
            LOG.info(sql);
            conn = ImpalaUtil.getConnection();
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
                if ("avgtimes".equals(index) || "active".equals(index)) {
                    vo.setValue(AnalysisCommonUtils.formatNumber(rs.getFloat(3)));
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
                    AnalysisVo vo2 = new AnalysisVo();
                    vo2.setDt(entry.getKey());
                    vo2.setName(xx.trim());
                    if ("avgtimes".equals(index) || "active".equals(index)) {
                        vo2.setValue("0.00");
                    } else {
                        vo2.setValue("0");
                    }
                    voList.add(vo2);
                }
            }
        }

        List<String> fullDateList = AnalysisCommonUtils.dateSplit(begin_date, end_date, dateSql);
        if (voList.size() < daysLen * subLength) {
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
                        if ("avgtimes".equals(index) || "active".equals(index)) {
                            vo2.setValue("0.00");
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
