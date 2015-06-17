/**
 * 
 */
package de.pavloff.spark4knime.jsnippet.type.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.knime.core.data.DataCell;

import de.pavloff.spark4knime.RddCell;
import de.pavloff.spark4knime.jsnippet.expression.TypeException;

/**
 * @author Oleg Pavlov
 *
 */
public class PairRDDValueToJava extends DataValueToJava {
	
	/**
     * Create a new instance.
     */
    public PairRDDValueToJava() {
        super(JavaPairRDD.class);
    }

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public boolean isCompatibleTo(DataCell cell, Class c) throws TypeException {
		return c.equals(JavaPairRDD.class)
	            && cell.getType() == RddCell.TYPE;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected Object getValueUnchecked(DataCell cell, Class c) {
		return ((RddCell)cell).getRDDValue();
	}

}
