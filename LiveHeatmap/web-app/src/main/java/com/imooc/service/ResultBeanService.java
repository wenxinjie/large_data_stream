package com.imooc.service;

//import com.google.common.util.concurrent.Service;
import com.imooc.domain.ResultBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
//import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@org.springframework.stereotype.Service
public class ResultBeanService {


    @Autowired
    JdbcTemplate jdbcTemplate;


    public List<ResultBean> query(int flag){

        String sql;
        if (flag == 1) {
            sql = "select lon, lat, count(1) as c from stat where time > unix_timestamp(date_sub(current_timestamp(), interval 20 second)) group by lon,lat";
        } else if (flag == 0){
            sql = "select lon, lat, count(1) as c from stat group by lon,lat";
        } else {
            sql = "select lon, lat, count(1) as c, sum(pos)/(sum(neg)+sum(pos)) as sent from sent_stat group by lon,lat";
        }

        return (List<ResultBean>) jdbcTemplate.query(sql, new RowMapper<ResultBean>() {

            @Override
            public ResultBean mapRow(ResultSet resultSet, int i) throws SQLException {
                ResultBean bean = new ResultBean();

                bean.setLng(resultSet.getDouble("lon"));
                bean.setLat(resultSet.getDouble("lat"));
                bean.setCount(resultSet.getLong("c"));

                if (flag > 1) {
                    bean.setSent(resultSet.getDouble("sent"));
                }

                return bean;
            }
        });
    }


}
