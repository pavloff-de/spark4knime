package de.pavloff.spark4knime;

import org.apache.spark.api.java.JavaRDD;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 * Implementation of a DataCell for saving an JavaRDD object into
 * BufferedDataTable
 * 
 * @see DataCell
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class RddCell extends DataCell implements RddValue {

	private static final long serialVersionUID = 2926662445712660558L;

	/**
	 * Convenience access member for
	 * <code>DataType.getType(DoubleCell.class)</code>.
	 * 
	 * @see DataType#getType(Class)
	 */
	public static final DataType TYPE = DataType.getType(RddCell.class);

	@SuppressWarnings("rawtypes")
	private final JavaRDD m_rdd;

	/**
	 * Creates a new cell for RDD.
	 * 
	 * @param rdd
	 *            The JavaRDD
	 */
	@SuppressWarnings("rawtypes")
	public RddCell(final JavaRDD rdd) {
		m_rdd = rdd;
	}

	/**
	 * Return RDD from cell.
	 * 
	 * @return JavaRDD
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public JavaRDD getRDDValue() {
		return m_rdd;
	}

	/**
	 * Represent RDD as a String.
	 */
	@Override
	public String toString() {
		return m_rdd.toString();
	}

	/**
	 * Compare RDD with other DataCell.
	 */
	@Override
	protected boolean equalsDataCell(DataCell dc) {
		if (dc.getType() != TYPE) {
			return false;
		}
		return m_rdd.equals(((RddCell) dc).getRDDValue());
	}

	/**
	 * Generate a hash code of RDD object.
	 */
	@Override
	public int hashCode() {
		return m_rdd.hashCode();
	}

}