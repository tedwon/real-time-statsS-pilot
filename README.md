real-time-statsS-pilot
======================

Real-time Analytics Pilot Project on SNS Big Data

http://www.facebook.com/groups/realtimecep/
http://www.facebook.com/groups/jbossusergroup/

See http://storm-project.net

See http://esper.codehaus.org/tutorials/tutorial/quickstart.html

See http://tedwon.com/display/dev/CEP

See http://tedwon.com/display/dev/Twitter+Storm


Prerequisites for Building
-------------------

Java JDK 1.6

Maven 2.2 or higher (http://maven.apache.org/)



Build
-------------------

$ mvn clean package



Run in local mode
-------------------

$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.realtimecep.pilots.analytics.sns.LocalTopologyStarter -Dexec.args="<twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379"

or

$ java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.pilots.analytics.sns.LocalTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379


Run in cluster mode
-------------------

Start

$ storm jar rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.realtimecep.pilots.analytics.sns.ClusterTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379


Stop

$ storm kill statss-analytics-topology