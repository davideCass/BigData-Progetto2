Avvio Progetto

Per avviare il progetto bisogna:
- avviare Kafka e creare il topic ‘numtest’ (kafka-topics.sh --create --zookeepeer localhost:2181 --replication-factor 1 --partitions 1 --    topic numtest)
- avviare il servizio di MongoDB 
- lanciare lo script start-project.sh.

Il dataset sul quale vengono eseguite le analisi è una versione ridotta
dei giorni dal 10 al 16 Febbraio 2017.
