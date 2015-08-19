/**
 * 
 */
package de.pavloff.spark4knime;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.util.ObjectToDataCellConverter;
import org.knime.core.internal.CorePlugin;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.tableview.TableContentModel;
import org.knime.core.node.tableview.TableView;

import scala.Tuple2;

/**
 * Common static class for communication between Spark nodes via
 * BufferedDataTable. Table contains a single RddCell with JavaRDDLike object.
 * <table>
 * <col width="25%"/> <col width="75%"/> <tbody>
 * <tr>
 * <td></td>
 * <td>RDD</td>
 * </tr>
 * <tr>
 * <td>RDD</td>
 * <td>JavaRDDLike object</td>
 * </tr>
 * </tbody>
 * </table>
 * 
 * @see BufferedDataTable
 * 
 * @author Oleg Pavlov
 */
public class TableCellUtils {

	/**
	 * Save an RDD object in BufferedDataTable via ExecutionContext
	 * 
	 * @param exec
	 *            <code>ExecutionContext</code>
	 * @param rdd
	 *            <code>JavaRDDLike</code> to save in table
	 * @throws NullPointerException
	 *             If rdd is null
	 * @return <code>BufferedDataTable</code> with a single Cell containing rdd
	 */
	public static BufferedDataTable setRDD(final ExecutionContext exec,
			@SuppressWarnings("rawtypes") JavaRDD rdd) {
		if (rdd == null) {
			throw new NullPointerException("RDD shouldn't be null");
		}
		BufferedDataContainer c = exec.createDataContainer(new DataTableSpec(
				new DataColumnSpecCreator("RDD", DataType
						.getType(RddCell.class)).createSpec()));
		c.addRowToTable(new DefaultRow(new RowKey("RDD"), new RddCell(rdd)));
		c.close();

		return c.getTable();
	}

	/**
	 * Save an PairRDD object in BufferedDataTable via ExecutionContext
	 * 
	 * @param exec
	 *            <code>ExecutionContext</code>
	 * @param rdd
	 *            <code>JavaRDDLike</code> to save in table
	 * @throws NullPointerException
	 *             If rdd is null
	 * @return <code>BufferedDataTable</code> with a single Cell containing rdd
	 */
	public static BufferedDataTable setRDD(final ExecutionContext exec,
			@SuppressWarnings("rawtypes") JavaPairRDD rdd) {
		if (rdd == null) {
			throw new NullPointerException("PairRDD shouldn't be null");
		}
		BufferedDataContainer c = exec.createDataContainer(new DataTableSpec(
				new DataColumnSpecCreator("PairRDD", DataType
						.getType(PairRddCell.class)).createSpec()));
		c.addRowToTable(new DefaultRow(new RowKey("RDD"), new PairRddCell(rdd)));
		c.close();

		return c.getTable();
	}

	/**
	 * Save an RDD object in BufferedDataTable via ExecutionContext
	 * 
	 * @param exec
	 *            <code>ExecutionContext</code>
	 * @param rdd
	 *            <code>JavaRDDLike</code> to save in table
	 * @throws NullPointerException
	 *             If rdd is null
	 * @return <code>BufferedDataTable</code> with a single Cell containing rdd
	 */
	@SuppressWarnings("rawtypes")
	public static BufferedDataTable setRDD(final ExecutionContext exec,
			JavaRDDLike rdd, Boolean isPairRDD) {
		if (isPairRDD) {
			return setRDD(exec, (JavaPairRDD) rdd);
		} else {
			return setRDD(exec, (JavaRDD) rdd);
		}
	}

	/**
	 * Read an RDD from BufferedDataTable table
	 * 
	 * @param table
	 *            <code>BufferedDataTable</code>
	 * @throws ClassCastException
	 *             If table contains non JavaRDDLike
	 * @return <code>JavaRDDLike</code> saved in table
	 */
	@SuppressWarnings("rawtypes")
	public static JavaRDDLike getRDD(BufferedDataTable table) {
		DataCell dc = table.iterator().next().getCell(0);
		if (dc.getType() == RddCell.TYPE) {
			return ((RddCell) dc).getRDDValue();
		} else if (dc.getType() == PairRddCell.TYPE) {
			return ((PairRddCell) dc).getPairRDDValue();
		} else {
			throw new ClassCastException(
					"table contains non JavaRDDLike object");
		}
	}

	/**
	 * Returns true if table contain JavaPairRDD and false if it is JavaRDD
	 * 
	 * @param table
	 *            <code>BufferedDataTable</code>
	 * @throws IndexOutOfBoundsException
	 *             If table doesn't contain two cells
	 * @throws ClassCastException
	 *             If second cell is not of type Boolean
	 * @return
	 */
	public static Boolean isPairRDD(BufferedDataTable table) {
		String[] names = table.getSpec().getColumnNames();
		if (names.length != 1) {
			throw new IndexOutOfBoundsException(
					"table should contain only one cells");
		}
		CloseableRowIterator it = table.iterator();
		DataRow firstRow = it.next();
		try {
			RddCell rddCell = (RddCell) firstRow.getCell(0);
			return false;
		} catch (Exception e) {
			PairRddCell pairRddCell = (PairRddCell) firstRow.getCell(0);
			return true;
		}
	}

	/**
	 * Find out type of element
	 * 
	 * @param element
	 * @return <code>DataType</code> of element
	 */
	public static DataType getTypeOfElement(Object element) {
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
			throw new UnsupportedOperationException("Class "
					+ element.getClass().getName()
					+ " is not supported for this operation");
		}
	}

	/**
	 * Makes a BufferedDataTable from a List of pairs with an Iterable object as
	 * value. Value should contain objects of one type.
	 * 
	 * @param l
	 *            <code>List</code> of Tuple2 objects
	 * @param exec
	 *            <code>ExecutionContext</code> of KNIME
	 * @return <code>BufferedDataTable</code>
	 */
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public static BufferedDataTable listOfIterablePairsToTable(List l,
			final ExecutionContext exec) {

		Tuple2 t = (Tuple2) l.get(0);
		ObjectToDataCellConverter cellFactory = ObjectToDataCellConverter.INSTANCE;

		// find maximum length
		int l_max = 0;
		DataType dt = null;
		for (Object s : l) {
			t = (Tuple2) s;
			Iterator it = ((Iterable) t._2).iterator();
			int l_cur = 0;
			for (; it.hasNext(); l_cur++) {
				// find inner type
				Object n = it.next();
				if (dt == null) {
					dt = getTypeOfElement(n);
				}
			}
			if (l_max < l_cur) {
				l_max = l_cur;
			}
		}

		// create specs
		DataColumnSpec[] specs = new DataColumnSpec[l_max + 1];
		specs[0] = new DataColumnSpecCreator("Key", getTypeOfElement(t._1))
				.createSpec();
		for (int i = 1; i <= l_max; i++) {
			specs[i] = new DataColumnSpecCreator("Value " + i, dt).createSpec();
		}

		// create container
		BufferedDataContainer container = exec
				.createDataContainer(new DataTableSpec(specs));

		// fill rows
		int i = 0;
		for (Object s : l) {
			t = (Tuple2) s;

			DataCell[] cells = new DataCell[l_max + 1];
			cells[0] = cellFactory.createDataCell(t._1);

			Iterator it = ((Iterable) t._2).iterator();
			int c_cur = 1;
			while (it.hasNext()) {
				cells[c_cur] = cellFactory.createDataCell(it.next());
				c_cur++;
			}
			container.addRowToTable(new DefaultRow(new RowKey("Row " + i++),
					cells));
		}
		container.close();
		return container.getTable();

	}

	/**
	 * Makes a BufferedDataTable from a List of pairs
	 * 
	 * @param l
	 *            <code>List</code> of Tuple2 objects
	 * @param exec
	 *            <code>ExecutionContext</code> of KNIME
	 * @return <code>BufferedDataTable</code>
	 */
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public static BufferedDataTable listOfPairsToTable(List l,
			final ExecutionContext exec) {

		if (l.isEmpty()) {
			return null;
		}

		Tuple2 t = (Tuple2) l.get(0);
		ObjectToDataCellConverter cellFactory = ObjectToDataCellConverter.INSTANCE;

		// handle iterable objects
		// scala.collection.convert.Wrappers$IterableWrapper
		// list can be written in columns
		if (t._2 instanceof Iterable) {
			return listOfIterablePairsToTable(l, exec);
		}

		BufferedDataContainer container = exec
				.createDataContainer(new DataTableSpec(
						new DataColumnSpec[] {
								new DataColumnSpecCreator("Key",
										getTypeOfElement(t._1)).createSpec(),
								new DataColumnSpecCreator("Value",
										getTypeOfElement(t._2)).createSpec() }));

		int i = 0;
		for (Object s : l) {
			t = (Tuple2) s;
			DataRow row = new DefaultRow(new RowKey("Row " + i++),
					new DataCell[] { cellFactory.createDataCell(t._1),
							cellFactory.createDataCell(t._2) });
			container.addRowToTable(row);
		}
		container.close();

		return container.getTable();
	}

	/**
	 * Makes a BufferedDataTable from a List of simple elements
	 * 
	 * @param l
	 *            <code>List</code> of Tuple2 objects
	 * @param exec
	 *            <code>ExecutionContext</code> of KNIME
	 * @return <code>BufferedDataTable</code>
	 */
	@SuppressWarnings({ "rawtypes", "deprecation" })
	public static BufferedDataTable listOfElementsToTable(List l,
			final ExecutionContext exec) {

		if (l.isEmpty()) {
			return null;
		}

		BufferedDataContainer container = exec
				.createDataContainer(new DataTableSpec(
						new DataColumnSpec[] { new DataColumnSpecCreator(
								"Column 0", getTypeOfElement(l.get(0)))
								.createSpec() }));
		ObjectToDataCellConverter cellFactory = ObjectToDataCellConverter.INSTANCE;

		int i = 0;
		for (Object s : l) {
			container.addRowToTable(new DefaultRow(new RowKey("Row " + i++),
					new DataCell[] { cellFactory.createDataCell(s) }));
		}
		container.close();

		return container.getTable();
	}

	public static final class RddViewer {

		private int takeSize = 10;
		private final ExecutionContext m_exec;
		private final BufferedDataTable m_table;

		public RddViewer(final BufferedDataTable table,
				final ExecutionContext exec) {
			m_exec = exec;
			m_table = table;
		}

		@SuppressWarnings("rawtypes")
		private BufferedDataTable getTable(int numRows) {
			try {
				if (isPairRDD(m_table)) {
					return listOfPairsToTable(
							((JavaPairRDD) getRDD(m_table)).take(numRows),
							m_exec);

				} else {
					return listOfElementsToTable(
							((JavaRDD) getRDD(m_table)).take(numRows), m_exec);
				}
			} catch (Exception e) {
				// try to show nonRDD table
				return m_table;
			}
		}

		public TableView getTableView() {
			return getTableView(takeSize);
		}

		public TableView getTableView(int numRows) {
			TableView tableView = new TableView(new TableContentModel(
					getTable(numRows)));
			tableView.setWrapColumnHeader(CorePlugin.getInstance()
					.isWrapColumnHeaderInTableViews());
			tableView.setPreferredSizeDataDependent(true);
			return tableView;
		}
	}

}
