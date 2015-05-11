/**
 * 
 */
package de.pavloff.spark4knime;

import org.apache.spark.api.java.JavaRDD;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;

/**
 * Common static class for communication between Spark nodes via
 * BufferedDataTable. Table contains a single RddCell with JavaRDD object.
 * <table>
 * <col width="25%"/> <col width="75%"/> <tbody>
 * <tr>
 * <td></td>
 * <td>RDD</td>
 * </tr>
 * <tr>
 * <td>RDD</td>
 * <td>JavaRDD object</td>
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
	 * Save an JavaRDD object in BufferedDataTable via ExecutionContext exec
	 * 
	 * @param exec
	 *            <code>ExecutionContext</code>
	 * @param rdd
	 *            <code>JavaRDD</code> to save in table
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
	 * Read an JavaRDD from BufferedDataTable table
	 * 
	 * @param table
	 *            <code>BufferedDataTable</code>
	 * @throws IndexOutOfBoundsException
	 *             If table doesn't contain any DataCell
	 * @throws ClassCastException
	 *             If table contains non JavaRDD
	 * @throws IllegalArgumentException
	 *             If table contains more than one DataCell
	 * @return <code>JavaRDD</code> saved in table
	 */
	@SuppressWarnings("rawtypes")
	public static JavaRDD getRDD(BufferedDataTable[] table) {
		if (table.length == 0) {
			throw new IndexOutOfBoundsException("table should'n be empty");
		}
		if (table.length != 1) {
			throw new IllegalArgumentException(
					"table should contain only one cell");
		}
		BufferedDataTable data = table[0];
		CloseableRowIterator it = data.iterator();
		DataCell dc = it.next().getCell(0);
		RddCell c;
		try {
			c = (RddCell) dc;
		} catch (Exception e) {
			throw new ClassCastException("table contains non JavaRDD object");
		}
		return c.get_rdd();
	}

}
