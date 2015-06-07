package de.pavloff.spark4knime.commons;

import java.util.HashMap;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Create SparkContexts.
 * 
 * @author Oleg Pavlov
 * 
 */
public class SparkContexter {

	/**
	 * SparkContext objects in a current JVM cached
	 */
	private static HashMap<String, JavaSparkContext> sparkContext = new HashMap<String, JavaSparkContext>();

	private SparkContexter() {
		// not allowed to create new objects
	}

	/**
	 * Create and save SparkContexts. Set allowMultipleContexts if there are
	 * different master in current JVM.
	 * 
	 * @see SPARK-2243 Only one SparkContext may be running in this JVM. To
	 *      ignore this error, set spark.driver.allowMultipleContexts = true
	 * @param master
	 *            <code>String</code> to connect to
	 * @return <code>SparkContext</code>
	 */
	public static JavaSparkContext getSparkContext(String master) {
		JavaSparkContext sc = SparkContexter.sparkContext.get(master);
		if (sc == null) {
			if (SparkContexter.sparkContext.size() == 1) {
				for (JavaSparkContext prev : SparkContexter.sparkContext
						.values()) {
					prev.getConf().set("spark.driver.allowMultipleContexts",
							"true");
				}
			}
			sc = new JavaSparkContext(master, "KnimeSparkApplication-"
					+ master.hashCode());
			SparkContexter.sparkContext.put(master, sc);
		}
		return sc;
	}
	
	/**
	 * Return Spark Master which is used before
	 * 
	 * @param defaultMaster
	 * @return master used before or defaultMaster
	 */
	public static String getCurrentMaster(String defaultMaster) {
		String currentMaster = defaultMaster;
		JavaSparkContext sc = SparkContexter.sparkContext.get(currentMaster);
		if (sc == null) {
			if (SparkContexter.sparkContext.size() != 0) {
				currentMaster = (String) SparkContexter.sparkContext.keySet().toArray()[0];
			}
		}
		return currentMaster;
	}
}
