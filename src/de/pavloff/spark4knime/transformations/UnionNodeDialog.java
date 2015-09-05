package de.pavloff.spark4knime.transformations;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;

/**
 * <code>NodeDialog</code> for the "Union" Node. Creates a new RDD that contains
 * the union of two RDD's
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class UnionNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring Union node dialog. No options are required.
	 */
	protected UnionNodeDialog() {
		super();
	}
}
