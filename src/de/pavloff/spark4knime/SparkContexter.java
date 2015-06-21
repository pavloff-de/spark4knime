package de.pavloff.spark4knime;

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
	 * Create new SparkContext. No multiple contexts allowed.
	 * 
	 * @see SPARK-2243 Only one SparkContext may be running in this JVM. To
	 *      ignore this error, set spark.driver.allowMultipleContexts = true
	 * @param master
	 *            <code>String</code> to connect to
	 * @throws NullPointerException
	 *             If master is null
	 * @return <code>SparkContext</code>
	 */
	public static synchronized JavaSparkContext getSparkContext(String master) {
		if (sparkContext == null) {
			if (master == null) {
				throw new NullPointerException("Master should be not null");
			}
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
	public static String getMaster(String defaultMaster) {
		String currentMaster = defaultMaster;
		if (sparkContext != null) {
			currentMaster = sparkContext.master();
		}
		return currentMaster;
	}
}
