/**
 * 
 */
package de.pavloff.spark4knime;

import org.apache.spark.api.java.JavaPairRDD;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;

/**
 * Implementation of a DtaCell for saving an JavaPairRDD object into a
 * BufferedDataTable
 * 
 * @see DataCell
 * 
 * @author Oleg Pavlov
 */
public class PairRddCell extends DataCell implements PairRddValue {
	
	private static final long serialVersionUID = -4679728041083699185L;
	
	/**
	 * Convenience access member for
	 * <code>DataType.getType(DoubleCell.class)</code>.
	 * 
	 * @see DataType#getType(Class)
	 */
	public static final DataType TYPE = DataType.getType(PairRddCell.class);
	
	@SuppressWarnings("rawtypes")
	private final JavaPairRDD m_rdd;
	
	/**
	 * Creates a new cell for a Spark PairRDD.
	 * 
	 * @param rdd
	 *            The JavaRDD
	 */
	@SuppressWarnings("rawtypes")
	public PairRddCell(final JavaPairRDD rdd) {
		m_rdd = rdd;
	}

	/**
	 * Return Spark PairRDD from cell.
	 * 
	 * @return JavaRDD
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public JavaPairRDD getPairRDDValue() {
		return m_rdd;
	}

	/**
	 * Represent Spark PairRDD as a String.
	 */
	@Override
	public String toString() {
		return m_rdd.toString();
	}

	/**
	 * Compare PairRDD from other DataCell.
	 */
	@Override
	protected boolean equalsDataCell(DataCell dc) {
		if (dc.getType() != TYPE) {
			return false;
		}
		return m_rdd.equals(((PairRddCell) dc).getPairRDDValue());
	}

	/**
	 * Generate a hash code of PairRDD object.
	 */
	@Override
	public int hashCode() {
		return m_rdd.hashCode();
	}

}
