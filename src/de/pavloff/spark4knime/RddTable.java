/**
 * 
 */
package de.pavloff.spark4knime;

import org.apache.spark.api.java.JavaRDDLike;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;

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
public class RddTable {

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

}
