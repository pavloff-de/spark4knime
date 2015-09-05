package de.pavloff.spark4knime.transformations;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import de.pavloff.spark4knime.TableCellUtils;
import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * This is the model implementation of SortByKey. Returns RDD of (K, V) pairs
 * sorted by keys in ascending or descending order
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class SortByKeyNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(SortByKeyNodeModel.class);

	// viewer instance
	private RddViewer rddViewer;

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_ORDER = "Order";

	/** initial default count value. */
	static final Boolean DEFAULT_ORDER = false;

	// example value: the models count variable filled from the dialog
	// and used in the models execution method. The default components of the
	// dialog work with "SettingsModels".
	private final SettingsModelBoolean m_order = new SettingsModelBoolean(
			SortByKeyNodeModel.CFGKEY_ORDER, SortByKeyNodeModel.DEFAULT_ORDER);

	/**
	 * Constructor for the node model.
	 */
	protected SortByKeyNodeModel() {
		// input: BufferedDataTable with JavaRDD
		// output: BufferedDataTable with sorted by key JavaRDD
		super(1, 1);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		// sort and save RDD
		if (TableCellUtils.isPairRDD(inData[0])) {
			BufferedDataTable[] out;
			out = new BufferedDataTable[] { TableCellUtils.setRDD(exec,
					((JavaPairRDD) TableCellUtils.getRDD(inData[0]))
							.sortByKey(m_order.getBooleanValue()), true) };

			// update veiwer
			rddViewer = new RddViewer(out[0], exec);

			return out;

		} else {
			throw new IllegalArgumentException(
					"SortByKey is only for PairRDD available");
		}
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
		// check if user settings are available, fit to the incoming table
		// structure, and the incoming types are feasible for the node to
		// execute. If the node can execute in its current state return the spec
		// of its output data table(s) (if you can, otherwise an array with null
		// elements), or throw an exception with a useful user message

		return new DataTableSpec[] { null };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		// save user settings to the config object.

		m_order.saveSettingsTo(settings);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		// load (valid) settings from the config object. It can be safely
		// assumed that the settings are valided by the method below.

		m_order.loadSettingsFrom(settings);

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

		m_order.validateSettings(settings);

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
