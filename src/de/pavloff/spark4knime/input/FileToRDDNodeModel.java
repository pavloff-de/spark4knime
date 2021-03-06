package de.pavloff.spark4knime.input;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import de.pavloff.spark4knime.TableCellUtils;
import de.pavloff.spark4knime.SparkContexter;
import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * This is the model implementation of FileToRDD. Read a text file and
 * parallelize data by lines.
 * 
 * Model returns a Spark RDD which contains lines (\n incl.) of file as simple
 * entries.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class FileToRDDNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(FileToRDDNodeModel.class);

	// viewer instance
	private RddViewer rddViewer;

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_MASTER = "Spark Master";
	static final String CFGKEY_PATH = "File";

	/** initial default values */
	static final String DEFAULT_MASTER = "local[*]";
	static final String DEFAULT_PATH = "";

	private final SettingsModelString m_master = new SettingsModelString(
			FileToRDDNodeModel.CFGKEY_MASTER, FileToRDDNodeModel.DEFAULT_MASTER);

	private final SettingsModelString m_path = new SettingsModelString(
			FileToRDDNodeModel.CFGKEY_PATH, FileToRDDNodeModel.DEFAULT_PATH);

	/**
	 * Constructor for the node model.
	 */
	protected FileToRDDNodeModel() {
		// input: null
		// output: BufferedDataTable with JavaRDD
		super(0, 1);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IllegalArgumentException
	 *             If path to text file is not set
	 */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		// check path
		String path = m_path.getStringValue();
		if (path.length() == 0) {
			throw new IllegalArgumentException("Path to text file is empty");
		}
		
		// create sparkContext and RDD
		JavaSparkContext sparkContext = SparkContexter.getSparkContext(m_master
				.getStringValue());
		BufferedDataTable[] out = new BufferedDataTable[] { TableCellUtils
				.setRDD(exec, sparkContext.textFile(m_path.getStringValue()),
						false) };
		
		// update viewer
		rddViewer = new RddViewer(out[0], exec);

		return out;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// Code executed on reset. Models build during execute are cleared here.
		// Also data handled in load/saveInternals will be erased here.
		
		rddViewer = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
			throws InvalidSettingsException {
		// check if user settings are available, fit to the incoming
		// table structure, and the incoming types are feasible for the node
		// to execute. If the node can execute in its current state return
		// the spec of its output data table(s) (if you can, otherwise an array
		// with null elements), or throw an exception with a useful user message
		
		return new DataTableSpec[] { null };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		// save user settings to the config object.

		m_master.saveSettingsTo(settings);
		m_path.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		// load (valid) settings from the config object. It can be safely
		// assumed that the settings are valided by the method below.

		m_master.loadSettingsFrom(settings);
		m_path.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		// check if the settings could be applied to our model e.g. if the count
		// is in a certain range (which is ensured by the SettingsModel). Do not
		// actually set any values of any member variables.

		m_master.validateSettings(settings);
		m_path.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {
		// load internal data. Everything handed to output ports is loaded
		// automatically (data returned by the execute method, models loaded in
		// loadModelContent, and user settings set through loadSettingsFrom - is
		// all taken care of). Load here only the other internals that need to
		// be restored (e.g. data used by the views).
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {
		// save internal models. Everything written to output ports is saved
		// automatically (data returned by the execute method, models saved in
		// the saveModelContent, and user settings saved through saveSettingsTo
		// - is all taken care of). Save here only the other internals that need
		// to be preserved (e.g. data used by the views).
	}
	
	/**
	 * @return <code>RddViewer</code> of the model
	 */
	public RddViewer getRddViewer() {
		return rddViewer;
	}

}
