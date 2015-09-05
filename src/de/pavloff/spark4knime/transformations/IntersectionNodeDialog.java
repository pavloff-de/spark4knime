package de.pavloff.spark4knime.transformations;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;

/**
 * <code>NodeDialog</code> for the "Intersection" Node. Creates an Intersection
 * of two RDD's
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class IntersectionNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring Intersection node dialog. No options are
	 * required.
	 */
	protected IntersectionNodeDialog() {
		super();
	}
}
