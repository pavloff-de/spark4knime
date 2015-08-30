package de.pavloff.spark4knime.transformations;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;

/**
 * <code>NodeDialog</code> for the "Intersection" Node.
 * Create an Intersection of two RDD's
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
     * New pane for configuring Intersection node dialog.
     * This is just a suggestion to demonstrate possible default dialog
     * components.
     */
    protected IntersectionNodeDialog() {
        super();
    }
}

