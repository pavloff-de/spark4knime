package de.pavloff.spark4knime.actions;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;

/**
 * <code>NodeDialog</code> for the "Take" Node. Take first n elements of RDD
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class TakeNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring Take node dialog. Number of elements to take.
	 */
	protected TakeNodeDialog() {
		super();

		// count
		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(TakeNodeModel.CFGKEY_COUNT,
						TakeNodeModel.DEFAULT_COUNT, 1, Integer.MAX_VALUE),
				TakeNodeModel.CFGKEY_COUNT, 1, 5));

	}
}
