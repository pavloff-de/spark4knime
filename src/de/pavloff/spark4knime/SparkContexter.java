package de.pavloff.spark4knime;

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
}
