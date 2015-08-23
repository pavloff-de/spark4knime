package de.pavloff.spark4knime.output;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import de.pavloff.spark4knime.SparkContexter;
import de.pavloff.spark4knime.TableCellUtils;

/**
 * This is the model implementation of RDDToSequenceFile. Save RDD as Sequence
 * File
 * 
 * @author Oleg Pavlov
 */
public class RDDToSequenceFileNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(RDDToSequenceFileNodeModel.class);

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_PATH = "File";
	static final String CFGKEY_OVERWRITE = "Overwrite";

	/** initial default count value. */
	static final String DEFAULT_PATH = "";
	static final Boolean DEFAULT_OVERWRITE = false;

	// example value: the models count variable filled from the dialog
	// and used in the models execution method. The default components of the
	// dialog work with "SettingsModels".
	private final SettingsModelString m_path = new SettingsModelString(
			RDDToSequenceFileNodeModel.CFGKEY_PATH,
			RDDToSequenceFileNodeModel.DEFAULT_PATH);
	private final SettingsModelBoolean m_overwrite = new SettingsModelBoolean(
			RDDToSequenceFileNodeModel.CFGKEY_OVERWRITE,
			RDDToSequenceFileNodeModel.DEFAULT_OVERWRITE);

	/**
	 * Constructor for the node model.
	 */
	protected RDDToSequenceFileNodeModel() {

		// TODO one incoming port and one outgoing port is assumed
		super(1, 0);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IllegalArgumentException
	 *             If path is empty
	 */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		String path = m_path.getStringValue();
		if (path.length() == 0) {
			throw new IllegalArgumentException("Path shouldn't be empty");
		}

		if (m_overwrite.getBooleanValue()) {
			if (path.startsWith("hdfs://")) {
				Path hdfsPath = new Path(path);
				FileSystem fs = FileSystem.get(new URI(path), SparkContexter
						.getSparkContext(null).hadoopConfiguration());

				if (fs.exists(hdfsPath)) {
					fs.delete(hdfsPath, true);
				}
			} else {
				File f = new File(path);

				if (f.exists()) {
					FileUtils.deleteDirectory(f);
				}
			}
		}

		TableCellUtils.getRDD(inData[0]).saveAsObjectFile(path);
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// TODO Code executed on reset.
		// Models build during execute are cleared here.
		// Also data handled in load/saveInternals will be erased here.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
			throws InvalidSettingsException {

		// TODO: check if user settings are available, fit to the incoming
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

		// TODO save user settings to the config object.

		m_path.saveSettingsTo(settings);
		m_overwrite.saveSettingsTo(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {

		// TODO load (valid) settings from the config object.
		// It can be safely assumed that the settings are valided by the
		// method below.

		m_path.loadSettingsFrom(settings);
		m_overwrite.loadSettingsFrom(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {

		// TODO check if the settings could be applied to our model
		// e.g. if the count is in a certain range (which is ensured by the
		// SettingsModel).
		// Do not actually set any values of any member variables.

		m_path.validateSettings(settings);
		m_overwrite.validateSettings(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {

		// TODO load internal data.
		// Everything handed to output ports is loaded automatically (data
		// returned by the execute method, models loaded in loadModelContent,
		// and user settings set through loadSettingsFrom - is all taken care
		// of). Load here only the other internals that need to be restored
		// (e.g. data used by the views).

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {

		// TODO save internal models.
		// Everything written to output ports is saved automatically (data
		// returned by the execute method, models saved in the saveModelContent,
		// and user settings saved through saveSettingsTo - is all taken care
		// of). Save here only the other internals that need to be preserved
		// (e.g. data used by the views).

	}

}
