/**
 * 
 */
package de.pavloff.spark4knime.jsnippet.type.data;

import org.apache.spark.api.java.JavaRDD;
import org.knime.core.data.DataCell;

import de.pavloff.spark4knime.RddCell;

/**
 * @author Oleg Pavlov
 *
 */
public class JavaToRddCell extends JavaToDataCell {
	
	/**
     * Create a new instance.
     */
    public JavaToRddCell() {
        super(JavaRDD.class);
    }

	/**
     * {@inheritDoc}
     */
	@SuppressWarnings("rawtypes")
	@Override
	protected DataCell createDataCellUnchecked(Object value) throws Exception {
		JavaRDD rdd = (JavaRDD) value;
		return new RddCell(rdd);
	}

}
