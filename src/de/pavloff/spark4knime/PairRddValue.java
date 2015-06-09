/**
 * 
 */
package de.pavloff.spark4knime;

import javax.swing.Icon;

import org.apache.spark.api.java.JavaPairRDD;
import org.knime.core.data.DataValue;
import org.knime.core.data.ExtensibleUtilityFactory;

/**
 * @author Oleg Pavlov
 *
 */
public interface PairRddValue extends DataValue {
	
	/** Meta information to this value type.
     * @see DataValue#UTILITY
     */
    UtilityFactory UTILITY = new RDDUtilityFactory();

    /**
     * @return A generic <code>PairRDD</code> value.
     */
    @SuppressWarnings("rawtypes")
	JavaPairRDD getPairRDDValue();
    
    /** Implementations of the meta information of this value class. */
    class RDDUtilityFactory extends ExtensibleUtilityFactory {
        /** Singleton icon to be used to display this cell type. */
        private static final Icon ICON = loadIcon(
        		RddValue.class, "/icon/rddicon.png");

        /** Only subclasses are allowed to instantiate this class. */
        protected RDDUtilityFactory() {
            super(PairRddValue.class);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Icon getIcon() {
            return ICON;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getName() {
            return "Spark PairRDD";
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getGroupName() {
            return "Spark";
        }
    }
}
