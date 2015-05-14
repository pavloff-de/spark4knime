package de.pavloff.spark4knime.transformations;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import de.pavloff.spark4knime.RddTable;

/**
 * This is the model implementation of Sample. Sample operation on Spark RDD
 * 
 * @author Oleg Pavlov
 */
public class SampleNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(SampleNodeModel.class);

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_REPLACEMENT = "With replacement";
	static final String CFGKEY_FRACTION = "Fraction of data (%)";
	static final String CFGKEY_SEED = "Random number generator";

	/** initial default count value. */
	static final Boolean DEFAULT_REPLACEMENT = false;
	static final int DEFAULT_FRACTION = 10;
	static final int DEFAULT_SEED = 1;

	// example value: the models count variable filled from the dialog
	// and used in the models execution method. The default components of the
	// dialog work with "SettingsModels".
	private final SettingsModelBoolean m_replacement = new SettingsModelBoolean(
			SampleNodeModel.CFGKEY_REPLACEMENT,
			SampleNodeModel.DEFAULT_REPLACEMENT);
	private final SettingsModelIntegerBounded m_fraction = new SettingsModelIntegerBounded(
			SampleNodeModel.CFGKEY_FRACTION, SampleNodeModel.DEFAULT_FRACTION,
			0, 100);
	private final SettingsModelIntegerBounded m_seed = new SettingsModelIntegerBounded(
			SampleNodeModel.CFGKEY_SEED, SampleNodeModel.DEFAULT_SEED,
			Integer.MIN_VALUE, Integer.MAX_VALUE);

	/**
	 * Constructor for the node model.
	 */
	protected SampleNodeModel() {
		// input: BufferedDataTable with JavaRDD
		// output: BufferedDataTable with Sample of JavaRDD
		super(1, 1);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		JavaRDDLike rdd;
		if (RddTable.isPairRDD(inData[0])) {
			rdd = ((JavaPairRDD) RddTable.getRDD(inData)).sample(
					m_replacement.getBooleanValue(),
					m_fraction.getIntValue() / 100.0, m_seed.getIntValue());
			return new BufferedDataTable[] { RddTable.setRDD(exec, rdd, true) };

		} else {
			rdd = ((JavaRDD) RddTable.getRDD(inData)).sample(
					m_replacement.getBooleanValue(),
					m_fraction.getIntValue() / 100.0, m_seed.getIntValue());
			return new BufferedDataTable[] { RddTable.setRDD(exec, rdd, false) };
		}
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

		m_replacement.saveSettingsTo(settings);
		m_fraction.saveSettingsTo(settings);
		m_seed.saveSettingsTo(settings);
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

		m_replacement.loadSettingsFrom(settings);
		m_fraction.loadSettingsFrom(settings);
		m_seed.loadSettingsFrom(settings);
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

		m_replacement.validateSettings(settings);
		m_fraction.validateSettings(settings);
		m_seed.validateSettings(settings);
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
