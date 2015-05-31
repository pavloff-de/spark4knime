package de.pavloff.spark4knime.actions;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.util.ObjectToDataCellConverter;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import scala.Tuple2;

import de.pavloff.spark4knime.RddTable;

/**
 * This is the model implementation of Collect. Collect all the elements of the
 * RDD as a table
 * 
 * @author Oleg Pavlov
 */
public class CollectNodeModel extends NodeModel {

	// the logger instance
	private static final NodeLogger logger = NodeLogger
			.getLogger(CollectNodeModel.class);

	/**
	 * Constructor for the node model.
	 */
	protected CollectNodeModel() {
		// input: BufferedDataTables with JavaRDD
		// output: BufferedDataTable with elements of JavaRDD
		super(1, 1);
	}

	/**
	 * Find out type of element
	 * 
	 * @param element
	 * @return <code>DataType</code> of element
	 */
	private DataType getTypeOfElement(Object element) {
		if (element instanceof Double) {
			return DoubleCell.TYPE;
		} else if (element instanceof Float) {
			return DoubleCell.TYPE;
		} else if (element instanceof String) {
			return StringCell.TYPE;
		} else if (element instanceof Integer) {
			return IntCell.TYPE;
		} else if (element instanceof Boolean) {
			return BooleanCell.TYPE;
		} else if (element instanceof Long) {
			return LongCell.TYPE;
		} else {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "rawtypes", "deprecation" })
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {

		List l;
		DataColumnSpec[] allColSpecs;
		ObjectToDataCellConverter cellFactory = ObjectToDataCellConverter.INSTANCE;
		BufferedDataTable out;
		BufferedDataContainer container;

		Boolean isPairRdd = RddTable.isPairRDD(inData[0]);
		if (isPairRdd) {
			l = ((JavaPairRDD) RddTable.getRDD(inData[0])).collect();
			if (l.isEmpty()) {
				return new BufferedDataTable[] { null };
			}
			
			Tuple2 t = (Tuple2) l.get(0);
			DataType elemType1 = getTypeOfElement(t._1);
			DataType elemType2 = getTypeOfElement(t._2);
			allColSpecs = new DataColumnSpec[2];
			allColSpecs[0] = new DataColumnSpecCreator("Column 0",
					elemType1).createSpec();
			allColSpecs[1] = new DataColumnSpecCreator("Column 1",
					elemType2).createSpec();
			container = exec.createDataContainer(new DataTableSpec(allColSpecs));
			
			int i = 0;
			for (Object s : l) {
				RowKey key = new RowKey("Row " + i++);
				DataCell[] cells = new DataCell[2];
				t = (Tuple2) s;
				cells[0] = cellFactory.createDataCell(t._1);
				cells[1] = cellFactory.createDataCell(t._2);
				DataRow row = new DefaultRow(key, cells);
				container.addRowToTable(row);
			}

		} else {
			l = ((JavaRDD) RddTable.getRDD(inData[0])).collect();
			if (l.isEmpty()) {
				return new BufferedDataTable[] { null };
			}
			
			DataType elemType = getTypeOfElement(l.get(0));
			allColSpecs = new DataColumnSpec[1];
			allColSpecs[0] = new DataColumnSpecCreator("Column 0",
					elemType).createSpec();
			container = exec.createDataContainer(new DataTableSpec(allColSpecs));
			
			int i = 0;
			for (Object s : l) {
				RowKey key = new RowKey("Row " + i++);
				DataCell[] cells = new DataCell[1];
				cells[0] = cellFactory.createDataCell(s);
				DataRow row = new DefaultRow(key, cells);
				container.addRowToTable(row);
			}
		}
		container.close();
		out = container.getTable();
		return new BufferedDataTable[] { out };
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
