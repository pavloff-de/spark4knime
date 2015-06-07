package de.pavloff.spark4knime.input;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import de.pavloff.spark4knime.commons.SparkContexter;

/**
 * <code>NodeDialog</code> for the "FileToRDD" Node. Read a text file and
 * parallelize data by lines
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov
 */
public class FileToRDDNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring FileToRDD node dialog. Spark master and path to
	 * text file are required.
	 */
	protected FileToRDDNodeDialog() {
		super();

		// master
		addDialogComponent(new DialogComponentString(new SettingsModelString(
				FileToRDDNodeModel.CFGKEY_MASTER,
				SparkContexter
						.getCurrentMaster(FileToRDDNodeModel.DEFAULT_MASTER)),
				FileToRDDNodeModel.CFGKEY_MASTER, true, 15));

		// text file
		addDialogComponent(new DialogComponentFileChooser(
				new SettingsModelString(FileToRDDNodeModel.CFGKEY_PATH,
						FileToRDDNodeModel.DEFAULT_PATH), "spark.file.reader"));
	}
}
