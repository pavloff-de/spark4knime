package de.pavloff.spark4knime.transformations;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;

/**
 * <code>NodeDialog</code> for the "Sample" Node. Sample operation on Spark RDD
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class SampleNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring Sample node dialog. This is just a suggestion to
	 * demonstrate possible default dialog components.
	 */
	protected SampleNodeDialog() {
		super();

		// replacement
		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(
				SampleNodeModel.CFGKEY_REPLACEMENT,
				SampleNodeModel.DEFAULT_REPLACEMENT),
				SampleNodeModel.CFGKEY_REPLACEMENT));

		// fraction
		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(
						SampleNodeModel.CFGKEY_FRACTION,
						SampleNodeModel.DEFAULT_FRACTION, 0, 100),
				SampleNodeModel.CFGKEY_FRACTION, 1, 5));

		// seed
		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(SampleNodeModel.CFGKEY_SEED,
						SampleNodeModel.DEFAULT_SEED, Integer.MIN_VALUE,
						Integer.MAX_VALUE), SampleNodeModel.CFGKEY_SEED, 1, 5));
	}
}
