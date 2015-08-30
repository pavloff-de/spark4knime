package de.pavloff.spark4knime.output;

import javax.swing.JFileChooser;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * <code>NodeDialog</code> for the "RDDToTextFile" Node. Save RDD to Text File
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class RDDToTextFileNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring RDDToTextFile node dialog. This is just a
	 * suggestion to demonstrate possible default dialog components.
	 */
	protected RDDToTextFileNodeDialog() {
		super();

		// text file
		addDialogComponent(new DialogComponentFileChooser(
				new SettingsModelString(RDDToTextFileNodeModel.CFGKEY_PATH,
						RDDToTextFileNodeModel.DEFAULT_PATH),
				"spark.file.writer", JFileChooser.OPEN_DIALOG, true));

		// overwrite
		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(
				RDDToTextFileNodeModel.CFGKEY_OVERWRITE,
				RDDToTextFileNodeModel.DEFAULT_OVERWRITE),
				"Overwrite if the directory already exists ?"));

	}
}
