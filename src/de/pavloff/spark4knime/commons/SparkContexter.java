package de.pavloff.spark4knime.commons;

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
	private static JavaSparkContext sparkContext;

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
		if (sparkContext == null) {
			sparkContext = new JavaSparkContext(master, "KnimeSparkApplication");
		}
		return sparkContext;
	}
	
	/**
	 * Return Spark Master which is used before
	 * 
	 * @param defaultMaster
	 * @return master used before or defaultMaster
	 */
	public static String getCurrentMaster(String defaultMaster) {
		String currentMaster = defaultMaster;
		if (sparkContext != null) {
			return sparkContext.master();
		}
		return currentMaster;
	}
}
