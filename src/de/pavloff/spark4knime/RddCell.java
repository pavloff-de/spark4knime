/**
 * 
 */
package de.pavloff.spark4knime;

import org.apache.spark.api.java.JavaRDDLike;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 * Implementation of a DtaCell for saving an JavaRDD object into a
 * BufferedDataTable
 * 
 * @see DataCell
 * 
 * @author Oleg Pavlov
 */
public class RddCell extends DataCell {

	private static final long serialVersionUID = 2926662445712660558L;

	/**
	 * Convenience access member for
	 * <code>DataType.getType(DoubleCell.class)</code>.
	 * 
	 * @see DataType#getType(Class)
	 */
	public static final DataType TYPE = DataType.getType(RddCell.class);

	@SuppressWarnings("rawtypes")
	private final JavaRDDLike m_rdd;

	/**
	 * Creates a new cell for a Spark RDD.
	 * 
	 * @param rdd
	 *            The JavaRDD
	 */
	@SuppressWarnings("rawtypes")
	public RddCell(final JavaRDDLike rdd) {
		m_rdd = rdd;
	}

	/**
	 * Return Spark RDD from cell.
	 * 
	 * @return JavaRDD
	 */
	@SuppressWarnings("rawtypes")
	public JavaRDDLike get_rdd() {
		return m_rdd;
	}

	/**
	 * Represent Spark RDD as a String.
	 */
	@Override
	public String toString() {
		return m_rdd.toString();
	}

	/**
	 * Compare RDD from other DataCell.
	 */
	@Override
	protected boolean equalsDataCell(DataCell dc) {
		if (dc.getType() != TYPE) {
			return false;
		}
		return m_rdd.equals(((RddCell) dc).get_rdd());
	}

	/**
	 * Generate a hash code of RDD object.
	 */
	@Override
	public int hashCode() {
		return m_rdd.hashCode();
	}
}