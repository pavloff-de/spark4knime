package de.pavloff.spark4knime.transformations;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;

/**
 * <code>NodeDialog</code> for the "GroupByKey" Node. When called on a dataset
 * of (K, V) pairs, returns a RDD of (K, Iterable<V>) pairs
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class GroupByKeyNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring GroupByKey node dialog. No options are required.
	 * components.
	 */
	protected GroupByKeyNodeDialog() {
		super();
	}
}
