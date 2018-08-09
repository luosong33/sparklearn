idea
打包：clean package
运行：spark2-submit --master spark://SH-M1-L06-YH-node1:7077 --class yyyq.rm.HastenMatching --name HastenMatching --executor-memory 6g --executor-cores 4 --num-executors 4 /bigdata/production/bgrm-1.0.0.jar
      spark2-submit --master yarn --class yyyq.rm.HastenMatching --executor-memory 10g --num-executors 5 /bigdata/production/bgrm-1.0.0.jar