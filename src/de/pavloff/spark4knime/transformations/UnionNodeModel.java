package de.pavloff.spark4knime.transformations;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import de.pavloff.spark4knime.TableCellUtils;

/**
 * This is the model implementation of Union. Creates a new RDD that contains
 * the union of two RDD's
 * 
 * @author Oleg Pavlov
 */
public class UnionNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(UnionNodeModel.class);

	/**
	 * Constructor for the node model.
	 */
	protected UnionNodeModel() {
		// input: two BufferedDataTables with JavaRDD
		// output: BufferedDataTable with union of JavaRDD's
		super(2, 1);
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {
		if (inData.length != 2) {
			throw new IndexOutOfBoundsException(
					"inData should contain two tables each with JavaRDDLike");
		}
		
		JavaRDDLike rdd;
		if (TableCellUtils.isPairRDD(inData[0])) {
			if (TableCellUtils.isPairRDD(inData[1])) {
				rdd = ((JavaPairRDD) TableCellUtils.getRDD(inData[0])).union((JavaPairRDD) TableCellUtils.getRDD(inData[1]));
				return new BufferedDataTable[] { TableCellUtils.setRDD(exec, rdd, true) };
			} else {
				throw new IllegalArgumentException("RDD's must be of same type");
			}
		} else {
			if (TableCellUtils.isPairRDD(inData[1])) {
				throw new IllegalArgumentException("RDD's must be of same type");
			} else {
				rdd = ((JavaRDD) TableCellUtils.getRDD(inData[0])).union((JavaRDD) TableCellUtils.getRDD(inData[1]));
				return new BufferedDataTable[] { TableCellUtils.setRDD(exec, rdd, false) };
			}
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

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {

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
