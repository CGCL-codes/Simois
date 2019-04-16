package com.basic.core.util;

import com.basic.core.inter.DumpRemoveHandler;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static com.basic.core.Constraints.*;

/**
 * locate com.basic.storm.util
 * Created by windy on 2019/1/2.
 */
public class PredictHeavyLoadKeyUtil implements Serializable{

    private static volatile PredictHeavyLoadKeyUtil predictHeavyLoadKeyUtil = null;

    private SynopsisHashMap<String, BitSet> predictHeavyLoadKeyMap = new SynopsisHashMap<String, BitSet>();

    private long dumpKeyCount=0L;
(PredictHeavyLoadKeyUtil.class);

    private long totalDelayTime=0L;// The total actual delay of the winning key
    private long totalKeyCount=0L;// Statistics the total number of winning keys

    private Random random=new Random();

    public PredictHeavyLoadKeyUtil() {
    }

    public static PredictHeavyLoadKeyUtil getPredictHeavyLoadKeyUtilInstance(){
        if(null == predictHeavyLoadKeyUtil)
        {
            synchronized (PredictHeavyLoadKeyUtil.class)
            {
                predictHeavyLoadKeyUtil=new PredictHeavyLoadKeyUtil();
            }
        }
        return predictHeavyLoadKeyUtil;
    }

    /**
     *  Throw coins until the coin looks up before the number of coins cast
     *  @return
     */
    public int countCointUtilUp(){
        int rand = (int)(Math.random()*2);
        int count = 0;
        while(rand == 0 && count < Threshold_l)     //Max length set equal to max length+r;
        {
            rand = (int)(Math.random()*2);
            count++;
        }
        return count;
    }

   
    public void SynopsisHashMapAllDump(DumpRemoveHandler dumpRemoveHandler) {
        int dumpsize = (int) (1 / Threshold_p);
        dumpKeyCount++;
        if (dumpKeyCount == dumpsize) {
            //dump all key
            Iterator<Map.Entry<String, BitSet>> iterator = predictHeavyLoadKeyMap.newEntryIterator();
            while (iterator.hasNext()){
                Map.Entry<String, BitSet> next = iterator.next();
                BitSet bitm = next.getValue();
                String key = next.getKey();
                if(key!=null){
                    long[] lo = bitm.toLongArray();
                    if(lo.length > 0){
                        for(int j=0;j<lo.length - 1;j++){
                            lo[j] = lo[j] >>> 1;
                            lo[j] = lo[j] | (lo[j+1] << 63);
                        }
                        lo[lo.length-1] = lo[lo.length-1] >>> 1;
                    }
                    bitm = BitSet.valueOf(lo);
                    if (bitm.isEmpty()) {
                        iterator.remove();
                        dumpRemoveHandler.dumpRemove(key);
                    }else
                        next.setValue(bitm);
                }
            }
            dumpKeyCount = 0;
        }
    }

        public void SynopsisHashMapRandomDump(DumpRemoveHandler dumpRemoveHandler) {
        int size=predictHeavyLoadKeyMap.size;
        long startTimeSystemTime=System.currentTimeMillis();
        Iterator<Map.Entry<String, BitSet>> iterator = predictHeavyLoadKeyMap.newEntryIterator();
        while (iterator.hasNext()){
            Map.Entry<String, BitSet> next = iterator.next();
            if (random.nextDouble()> Threshold_p){
                continue;
            }
            BitSet bitm = next.getValue();
            String key = next.getKey();
            if(key!=null){
                long[] lo = bitm.toLongArray();
                if(lo.length > 0){
                    for(int j=0;j<lo.length - 1;j++){
                        lo[j] = lo[j] >>> 1;
                        lo[j] = lo[j] | (lo[j+1] << 63);
                    }
                    lo[lo.length-1] = lo[lo.length-1] >>> 1;
                }
                bitm = BitSet.valueOf(lo);
                if (bitm.isEmpty()) {
                    iterator.remove();
                    dumpRemoveHandler.dumpRemove(key);
                }else
                    next.setValue(bitm);
            }
        }
    }

    /**
     * PredictHeavyLoadKey
     * @param key
     * @param coninCount
     */
    public void PredictHeavyLoadKey(String key,int coninCount){
        int count=coninCount-Threshold_r;
        BitSet bitmap=null;
        if(predictHeavyLoadKeyMap.get(key)!=null)
            bitmap = (BitSet) predictHeavyLoadKeyMap.get(key);
        else
            bitmap=new BitSet(Threshold_l);

        bitmap.set(coninCount);
        predictHeavyLoadKeyMap.put(key,bitmap);

        if(bitmap.cardinality() >= 2)
        {
                  }
    }

     public void simpleComputPredictHeavyLoadKey(String key) {
        int count = countCointUtilUp();
        int dumpsize = (int) (1 / Threshold_p);

        if (count >= Threshold_r) {
            PredictHeavyLoadKey(key, count - Threshold_r);
            SynopsisHashMapAllDump(new DumpRemoveHandler() {
                @Override
                public void dumpRemove(String key) {

                }
            });
        }
    }


    public boolean isHeavyLoadKey(String key){
       if(!predictHeavyLoadKeyMap.containsKey(key))
           return false;
        if(predictHeavyLoadKeyMap.get(key).cardinality() >= 2)
            return true;
        else
            return false;
    }

    public long getTotalDelayTime() {
        return totalDelayTime;
    }

    public void setTotalDelayTime(long totalDelayTime) {
        this.totalDelayTime = totalDelayTime;
    }

    public long getTotalKeyCount() {
        return totalKeyCount;
    }

    public void setTotalKeyCount(long totalKeyCount) {
        this.totalKeyCount = totalKeyCount;
    }

    public SynopsisHashMap<String, BitSet> getPredictHeavyLoadKeyMap() {
        return predictHeavyLoadKeyMap;
    }

    public void setPredictHeavyLoadKeyMap(SynopsisHashMap<String, BitSet> predictHeavyLoadKeyMap) {
        this.predictHeavyLoadKeyMap = predictHeavyLoadKeyMap;
    }
}
