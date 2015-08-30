package de.pavloff.spark4knime.output;

import javax.swing.JFileChooser;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * <code>NodeDialog</code> for the "RDDToSequenceFile" Node. Save elements of
 * the RDD as Hadoop Sequence File.
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class RDDToSequenceFileNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring RDDToSequenceFile node dialog. Path to a non
	 * existing folder is required. Overwrite parameter removes an existing
	 * folder before write.
	 */
	protected RDDToSequenceFileNodeDialog() {
		super();

		// text file
		addDialogComponent(new DialogComponentFileChooser(
				new SettingsModelString(RDDToTextFileNodeModel.CFGKEY_PATH,
						RDDToSequenceFileNodeModel.DEFAULT_PATH),
				"spark.sequencefile.writer", JFileChooser.OPEN_DIALOG, true));

		// overwrite
		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(
				RDDToSequenceFileNodeModel.CFGKEY_OVERWRITE,
				RDDToSequenceFileNodeModel.DEFAULT_OVERWRITE),
				"Overwrite if the directory already exists ?"));

	}
}
