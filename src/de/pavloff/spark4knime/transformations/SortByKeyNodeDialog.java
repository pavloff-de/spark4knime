package de.pavloff.spark4knime.transformations;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;

/**
 * <code>NodeDialog</code> for the "SortByKey" Node. returns a dataset of (K, V)
 * pairs sorted by keys in ascending or descending order
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov
 */
public class SortByKeyNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring SortByKey node dialog. This is just a suggestion
	 * to demonstrate possible default dialog components.
	 */
	protected SortByKeyNodeDialog() {
		super();

		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(
				SortByKeyNodeModel.CFGKEY_ORDER,
				SortByKeyNodeModel.DEFAULT_ORDER), "descending order"));

	}
}
