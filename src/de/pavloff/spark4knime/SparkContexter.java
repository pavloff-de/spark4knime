package de.pavloff.spark4knime;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.knime.core.util.FileUtil;

/**
 * Singleton class creates SparkContext
 * 
 * @use SparkContexter.getSparkContext(MASTER)
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class SparkContexter {

	/**
	 * SparkContext objects cached in a current JVM
	 */
	private static JavaSparkContext sparkContext;

	private SparkContexter() {
		// not allowed to create new objects
	}

	/**
	 * Create new SparkContext if not already created and cache it for next use.
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
					// some operations need Apache Hadoop
					// HADOOP_HOME is set if not exists
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
					// need to overwrite JSnippet classes
					.set("spark.files.overwrite", "true");
			sparkContext = new JavaSparkContext(conf);
		}
		return sparkContext;
	}

	/**
	 * Spark Master connected to
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
