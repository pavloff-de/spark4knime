package de.pavloff.spark4knime.input;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import de.pavloff.spark4knime.SparkContexter;

/**
 * <code>NodeDialog</code> for the "TableToRDD" Node. Read a table from previous
 * node as input and parallelize data by rows.
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class TableToRDDNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring TableToRDD node dialog. Spark master is
	 * required. Column chooser is used to filter out the columns, that will be
	 * parallelized.
	 */
	protected TableToRDDNodeDialog() {
		super();

		// master
		addDialogComponent(new DialogComponentString(new SettingsModelString(
				TableToRDDNodeModel.CFGKEY_MASTER,
				SparkContexter.getMaster(TableToRDDNodeModel.DEFAULT_MASTER)),
				TableToRDDNodeModel.CFGKEY_MASTER, true, 15));

		// column chooser
		addDialogComponent(new DialogComponentColumnFilter2(
				new SettingsModelColumnFilter2(TableToRDDNodeModel.CFG_COLUMNS),
				0));
	}
}
