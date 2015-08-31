package de.pavloff.spark4knime.actions;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;

/**
 * <code>NodeDialog</code> for the "CountByKey" Node. Make a hashmap of (Key,
 * Int) pairs with the count of each key. Only for pairRDD available.
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class CountByKeyNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring Collect node dialog. No options are required.
	 */
	protected CountByKeyNodeDialog() {
		super();
	}
}
