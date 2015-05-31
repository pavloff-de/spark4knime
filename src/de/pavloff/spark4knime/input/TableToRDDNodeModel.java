package de.pavloff.spark4knime.input;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import scala.Tuple2;

import de.pavloff.spark4knime.RddTable;
import de.pavloff.spark4knime.SparkContexter;

/**
 * This is the model implementation of TableToRDD. Read a table and parallelize
 * data by rows. Node model returns a Spark RDD which contains data from table
 * columns. PairRDD is returned if more than 1 column selected. Maximally first
 * two columns will be used. Columns should contain data of following types:
 * String, Double, Integer, Long, Boolean.
 * 
 * @author Oleg Pavlov
 */
public class TableToRDDNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(TableToRDDNodeModel.class);

	/**
	 * the settings key which is used to retrieve and store the settings (from
	 * the dialog or from a settings file) (package visibility to be usable from
	 * the dialog).
	 */
	static final String CFGKEY_MASTER = "Spark Master";
	static final String CFG_COLUMNS = "Columns to parallelize";

	/** initial default count value. */
	static final String DEFAULT_MASTER = "local[2]";

	private final SettingsModelString m_master = new SettingsModelString(
			TableToRDDNodeModel.CFGKEY_MASTER,
			TableToRDDNodeModel.DEFAULT_MASTER);
	private final SettingsModelColumnFilter2 m_columns = new SettingsModelColumnFilter2(
			TableToRDDNodeModel.CFG_COLUMNS);

	/**
	 * Constructor for the node model.
	 */
	protected TableToRDDNodeModel() {
		// input: some BufferedDataTable with data
		// output: BufferedDataTable with JavaRDD
		super(1, 1);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IndexOutOfBoundsException
	 *             If number of selected columns is zero
	 * @throws InvalidSettingsException
	 *             If no column with given name in table
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		BufferedDataTable data = inData[0];
		DataTableSpec dataSpec = data.getDataTableSpec();

		FilterResult filterResult = m_columns.applyTo(dataSpec);
		List<String> includes = Arrays.asList(filterResult.getIncludes());
		int numColumns = includes.size();
		String[] names = includes.toArray(new String[numColumns]);

		if (numColumns == 0) {
			throw new IndexOutOfBoundsException(
					"Number of columns should'n be zero");
		}
		if (numColumns > 2) {
			setWarningMessage("Only two first columns will be used");
		}

		final int[] colIndices = new int[names.length];
		for (int i = 0; i < names.length; i++) {
			int index = dataSpec.findColumnIndex(names[i]);
			if (index < 0) {
				throw new InvalidSettingsException("No column \"" + names[i]
						+ "\" in input table");
			}
			colIndices[i] = index;
		}

		JavaRDDLike rdd;
		if (numColumns == 1) {
			rdd = createRDD(data, colIndices);
			return new BufferedDataTable[] { RddTable.setRDD(exec, rdd, false) };
		} else {
			rdd = createPairRDD(data, colIndices);
			return new BufferedDataTable[] { RddTable.setRDD(exec, rdd, true) };
		}

	}

	/**
	 * Returns a DataCell value
	 * 
	 * @param cell
	 *            <code>DataCell</code>
	 * @return <code>Object</code> saved in cell
	 */
	private Object getCellValue(DataCell cell) {
		DataType type = cell.getType();

		if (type == StringCell.TYPE) {
			return ((StringCell) cell).getStringValue();
		} else if (type == DoubleCell.TYPE) {
			return ((DoubleCell) cell).getDoubleValue();
		} else if (type == IntCell.TYPE) {
			return ((IntCell) cell).getIntValue();
		} else if (type == LongCell.TYPE) {
			return ((LongCell) cell).getLongValue();
		} else if (type == BooleanCell.TYPE) {
			return ((BooleanCell) cell).getBooleanValue();
		} else {
			return null;
		}
	}

	/**
	 * Creates JavaRDD from BufferedDataTable
	 * 
	 * @param data
	 *            <code>BufferedDataTable</code> to parallelize
	 * @param colIndices
	 *            selected columns
	 * @return <code>JavaRDD</code>
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private JavaRDD createRDD(BufferedDataTable data, int[] colIndices) {
		JavaRDD rdd;
		CloseableRowIterator rowIt = data.iterator();
		DataCell firstCell = rowIt.next().getCell(colIndices[0]);

		JavaSparkContext sparkContext = SparkContexter.getSparkContext(m_master
				.getStringValue());
		ArrayList toParallelize = new ArrayList(1);
		toParallelize.add(getCellValue(firstCell));

		rdd = sparkContext.parallelize(toParallelize);
		ArrayList rest = new ArrayList();

		while (rowIt.hasNext()) {
			toParallelize = new ArrayList(1);
			DataRow nextRow = rowIt.next();
			toParallelize.add(getCellValue(nextRow.getCell(colIndices[0])));
			rest.add(sparkContext.parallelize(toParallelize));
		}
		return sparkContext.union(rdd, rest);
	}

	/**
	 * Creates JavaPairRDD from BufferedDataTable
	 * 
	 * @param data
	 *            <code>BufferedDataTable</code> to parallelize
	 * @param colIndices
	 *            selected columns
	 * @return <code>JavaPairRDD</code>
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private JavaPairRDD createPairRDD(BufferedDataTable data, int[] colIndices) {
		JavaPairRDD rdd;
		CloseableRowIterator rowIt = data.iterator();
		DataRow firstRow = rowIt.next();

		JavaSparkContext sparkContext = SparkContexter.getSparkContext(m_master
				.getStringValue());
		ArrayList toParallelize = new ArrayList(1);
		toParallelize.add(new Tuple2(getCellValue(firstRow
				.getCell(colIndices[0])), getCellValue(firstRow
				.getCell(colIndices[1]))));

		rdd = JavaPairRDD.fromJavaRDD(sparkContext.parallelize(toParallelize));
		ArrayList rest = new ArrayList();

		while (rowIt.hasNext()) {
			toParallelize = new ArrayList(1);
			DataRow nextRow = rowIt.next();
			toParallelize.add(new Tuple2(getCellValue(nextRow
					.getCell(colIndices[0])), getCellValue(nextRow
					.getCell(colIndices[1]))));
			rest.add(JavaPairRDD.fromJavaRDD(sparkContext
					.parallelize(toParallelize)));
		}

		return sparkContext.union(rdd, rest);
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

		m_master.saveSettingsTo(settings);
		m_columns.saveSettingsTo(settings);
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

		m_master.loadSettingsFrom(settings);
		m_columns.loadSettingsFrom(settings);
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

		m_master.validateSettings(settings);
		m_columns.validateSettings(settings);
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
