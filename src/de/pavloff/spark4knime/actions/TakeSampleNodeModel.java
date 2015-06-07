package de.pavloff.spark4knime.actions;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

import de.pavloff.spark4knime.commons.TableCellUtils;

/**
 * This is the model implementation of TakeSample. Returns a sample of RDD as
 * list
 * 
 * @author Oleg Pavlov
 */
public class TakeSampleNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(TakeSampleNodeModel.class);

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_REPLACEMENT = "With replacement";
	static final String CFGKEY_COUNT = "Number of data";
	static final String CFGKEY_SEED = "Random number generator";

	/** initial default count value. */
	static final Boolean DEFAULT_REPLACEMENT = false;
	static final int DEFAULT_COUNT = 10;
	static final int DEFAULT_SEED = 1;

	// example value: the models count variable filled from the dialog
	// and used in the models execution method. The default components of the
	// dialog work with "SettingsModels".
	private final SettingsModelBoolean m_replacement = new SettingsModelBoolean(
			TakeSampleNodeModel.CFGKEY_REPLACEMENT,
			TakeSampleNodeModel.DEFAULT_REPLACEMENT);
	private final SettingsModelIntegerBounded m_count = new SettingsModelIntegerBounded(
			TakeSampleNodeModel.CFGKEY_COUNT,
			TakeSampleNodeModel.DEFAULT_COUNT, 1, Integer.MAX_VALUE);
	private final SettingsModelIntegerBounded m_seed = new SettingsModelIntegerBounded(
			TakeSampleNodeModel.CFGKEY_SEED, TakeSampleNodeModel.DEFAULT_SEED,
			Integer.MIN_VALUE, Integer.MAX_VALUE);

	/**
	 * Constructor for the node model.
	 */
	protected TakeSampleNodeModel() {
		// input: BufferedDataTables with JavaRDDLike
		// output: BufferedDataTable with sample elements
		super(1, 1);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		if (TableCellUtils.isPairRDD(inData[0])) {
			return new BufferedDataTable[] { TableCellUtils.listOfPairsToTable(
					((JavaPairRDD) TableCellUtils.getRDD(inData[0]))
							.takeSample(m_replacement.getBooleanValue(),
									m_count.getIntValue(),
									m_seed.getIntValue()), exec) };

		} else {
			return new BufferedDataTable[] { TableCellUtils
					.listOfElementsToTable(((JavaRDD) TableCellUtils
							.getRDD(inData[0])).takeSample(
							m_replacement.getBooleanValue(),
							m_count.getIntValue(), m_seed.getIntValue()), exec) };
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

		m_replacement.saveSettingsTo(settings);
		m_count.saveSettingsTo(settings);
		m_seed.saveSettingsTo(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {

		m_replacement.loadSettingsFrom(settings);
		m_count.loadSettingsFrom(settings);
		m_seed.loadSettingsFrom(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {

		m_replacement.validateSettings(settings);
		m_count.validateSettings(settings);
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
