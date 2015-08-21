package de.pavloff.spark4knime;

import java.io.IOException;
import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.util.FileUtil;
import org.osgi.framework.Bundle;

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
			if (System.getProperty("hadoop.home.dir") == null) {
				try {
					System.setProperty(
							"hadoop.home.dir",
							FileUtil.getFileFromURL(
									FileLocator.toFileURL(FileLocator.find(
											Platform.getBundle("de.pavloff.spark4knime"),
											new Path("hadoop"), null)))
									.getPath().toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			SparkConf conf = new SparkConf().setMaster(master)
					.setAppName("KnimeSparkApplication")
					.set("spark.files.overwrite", "true");
			sparkContext = new JavaSparkContext(conf);
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
