/**
 * 
 */
package de.pavloff.spark4knime;

import java.util.List;

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
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;

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
			@SuppressWarnings("rawtypes") JavaRDDLike rdd, Boolean isPairRDD) {
		if (rdd == null) {
			throw new NullPointerException("RDD shouldn't be null");
		}
		BufferedDataContainer c = exec.createDataContainer(new DataTableSpec(
				new DataColumnSpecCreator("RDD", DataType
						.getType(RddCell.class)).createSpec(),
				new DataColumnSpecCreator("pairRDD", DataType
						.getType(RddCell.class)).createSpec()));
		c.addRowToTable(new DefaultRow(new RowKey("RDD"), new RddCell(rdd),
				BooleanCell.get(isPairRDD)));
		c.close();

		return c.getTable();
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
		CloseableRowIterator it = table.iterator();
		DataCell dc = it.next().getCell(0);
		RddCell c;
		try {
			c = (RddCell) dc;
		} catch (Exception e) {
			throw new ClassCastException(
					"table contains non JavaRDDLike object");
		}
		return c.get_rdd();
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
		CloseableRowIterator it = table.iterator();
		DataRow row = it.next();
		if (row.getNumCells() != 2) {
			throw new IndexOutOfBoundsException(
					"table should contain two cells");
		}
		DataCell dc = row.getCell(1);
		BooleanCell c;
		try {
			c = (BooleanCell) dc;
		} catch (Exception e) {
			throw new ClassCastException(
					"table contains non JavaRDDLike object");
		}
		return c.getBooleanValue();
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
			return null;
		}
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

		BufferedDataContainer container = exec
				.createDataContainer(new DataTableSpec(new DataColumnSpec[] {
						new DataColumnSpecCreator("Column 0", TableCellUtils
								.getTypeOfElement(t._1)).createSpec(),
						new DataColumnSpecCreator("Column 1", TableCellUtils
								.getTypeOfElement(t._2)).createSpec() }));
		ObjectToDataCellConverter cellFactory = ObjectToDataCellConverter.INSTANCE;

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
								"Column 0", TableCellUtils.getTypeOfElement(l
										.get(0))).createSpec() }));
		ObjectToDataCellConverter cellFactory = ObjectToDataCellConverter.INSTANCE;

		int i = 0;
		for (Object s : l) {
			container.addRowToTable(new DefaultRow(new RowKey("Row " + i++),
					new DataCell[] { cellFactory.createDataCell(s) }));
		}
		container.close();

		return container.getTable();
	}

}
