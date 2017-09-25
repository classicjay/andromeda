package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by zg on 2017/8/23.
 */
@Service("KpiTableComputeTaskService")
@CrossOrigin(origins = "*")
public class KpiTableComputeTaskService extends ComputeTaskServiceImpl implements ComputeTaskService, EnvironmentAware {

    private static Logger log = LoggerFactory.getLogger(KpiTableComputeTaskService.class);

    @Autowired
    @Qualifier("QueryDataFromHbaseServiceImpl")
    private QueryDataService queryDataService;

    @Autowired
    private ComputeService computeService;

    @Autowired
    private Environment env;

    private ExecutorService executor;

    private int threadNum = 3;

    @Override
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }

    /**
     * 初始化系统变量
     */
    @PostConstruct
    public void init(){
        String threadNumStr =  env.getProperty("system.thread.threadNumStr");
        if(null == threadNumStr){
            threadNumStr = "3";
        }
        threadNum = Integer.parseInt(threadNumStr);
        int cpuNum = Runtime.getRuntime().availableProcessors();
        executor = new ThreadPoolExecutor(cpuNum, cpuNum * 2, 60000L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    public List<Map<String, Object>> run(DataObject dataObject) {
        //1.调用工具类查hdfs数据，数据量打，并发查询
        //每个账期调用引擎计算，考虑并发调用
        Map<String, List<String>> whereMap = dataObject.getWhereMap();
        List<String> keyList = super.generateRedisKeyList(dataObject);
        CompletionService completionService = new ExecutorCompletionService(executor);
        //List<Callable<Object>> taskList = new LinkedList<Callable<Object>>();
        //如果查询key的数目大于100，则多线程查询
        int keyListSize = keyList.size();
        List<String> redisResult = new ArrayList<>();
        //int threadNum = 10;
        int step = keyListSize / threadNum;
        int other = keyListSize % threadNum;
        long startTime = System.currentTimeMillis();
        if(keyListSize > step){
            /*for(int i=0; i * step < keyListSize; i++){
                List<Get> subList = keyList.subList(i*step, (i+1)*step > keyListSize ? keyListSize : (i+1)*step);
                taskList.add(new QueryHbaseTask(subList, dataObject));
            }*/
            for(int i=0; i  < threadNum; i++){
                List<String> subList = keyList.subList(i*step, (i == (threadNum-1) ? ((i+1)*step + other) : (i+1)*step));
//                log.info("分给单个线程的keylist长度"+subList.size());
                //taskList.add(new QueryHbaseTask(subList, dataObject));
                completionService.submit(new QueryRedisTask(subList));
            }
            try {
                /*List<Future<Object>> futures = executor.invokeAll(taskList);
                for (Future<Object> future : futures) {
                    List<String> result = (List<String>) future.get();
                    if (null != result) {
                        hbaseResult.addAll(result);
                    }
                }*/
                for(int i = 0; i < threadNum;i++ ){
                    redisResult.addAll((List<String>) completionService.take().get());
                }
            } catch (InterruptedException e) {
                log.error("", e);
            } catch (Exception e) {
                log.error("", e);
            }
        }else {
            redisResult = queryDataService.queryRedisData(keyList);
        }
        long endTime = System.currentTimeMillis();
        log.info("3个线程查hbase总时间"+(endTime - startTime)/1000 );

        if(redisResult!=null&&redisResult.size()>0){
            log.info("查询结果集："+redisResult.size());
        }
        long startTimeConvert = System.currentTimeMillis();
        //2.查询结果处理，本次循环遍历可以放到计算时做
        List<Map<String, Object>> resultSet = super.convertResult(dataObject, redisResult);
        log.info("转换结果集："+resultSet.size());
        long endTimeConvert = System.currentTimeMillis();
        log.info("转换耗时"+(endTimeConvert - startTimeConvert)/1000 );
        if(resultSet!=null&&resultSet.size()>0){
            log.info("转换结果："+resultSet.get(0));
        }
        //3.调用计算框架计算，计算压力过大，并发计算convertResult
        List<Map<String, Object>> resultList = computeService.compute(dataObject, resultSet);
        log.info("计算结果集："+resultList.size());
        if(resultList!=null&&resultList.size()>0){
            log.info("计算结果是："+resultList.get(0));
        }
        long startTimeProcess = System.currentTimeMillis();
        //4.处理结果，合并结果
        List<Map<String, Object>> completeList = super.processBlank(dataObject, resultList);
        long endTimeProcess = System.currentTimeMillis();
        log.info("空处理耗时"+(endTimeProcess - startTimeProcess)/1000 );
        return completeList;
    }
    /**
     * 复合指标并发任务类
     */
    class QueryRedisTask implements Callable {
        private List<String> keyList;

        public QueryRedisTask(List<String> keyList){
            this.keyList = keyList;
        }

        @Override
        public Object call() throws Exception {
            return queryDataService.queryRedisData(keyList);
        }
    }
}
