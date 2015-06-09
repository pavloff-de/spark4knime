/**
 * 
 */
package de.pavloff.spark4knime.jsnippet.type.data;

import org.apache.spark.api.java.JavaPairRDD;
import org.knime.core.data.DataCell;

import de.pavloff.spark4knime.PairRddCell;

/**
 * @author zipper
 *
 */
public class JavaToPairRddCell extends JavaToDataCell {
	
	/**
     * Create a new instance.
     */
    public JavaToPairRddCell() {
        super(JavaPairRDD.class);
    }

    /**
     * {@inheritDoc}
     */
	@SuppressWarnings("rawtypes")
	@Override
	protected DataCell createDataCellUnchecked(Object value) throws Exception {
		JavaPairRDD rdd = (JavaPairRDD) value;
		return new PairRddCell(rdd);
	}

}
