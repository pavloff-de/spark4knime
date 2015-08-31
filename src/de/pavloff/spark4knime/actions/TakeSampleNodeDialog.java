package de.pavloff.spark4knime.actions;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;

/**
 * <code>NodeDialog</code> for the "TakeSample" Node. Returns a random sample of
 * RDD as list
 * 
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class TakeSampleNodeDialog extends DefaultNodeSettingsPane {

	/**
	 * New pane for configuring TakeSample node dialog. Number of elements to
	 * take with or without replacement. A random number generator seed is
	 * optional.
	 */
	protected TakeSampleNodeDialog() {
		super();

		// replacement
		addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(
				TakeSampleNodeModel.CFGKEY_REPLACEMENT,
				TakeSampleNodeModel.DEFAULT_REPLACEMENT),
				TakeSampleNodeModel.CFGKEY_REPLACEMENT));

		// count
		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(
						TakeSampleNodeModel.CFGKEY_COUNT,
						TakeSampleNodeModel.DEFAULT_COUNT, 0, Integer.MAX_VALUE),
				TakeSampleNodeModel.CFGKEY_COUNT, 1, 5));

		// seed
		addDialogComponent(new DialogComponentNumber(
				new SettingsModelIntegerBounded(
						TakeSampleNodeModel.CFGKEY_SEED,
						TakeSampleNodeModel.DEFAULT_SEED, Integer.MIN_VALUE,
						Integer.MAX_VALUE), TakeSampleNodeModel.CFGKEY_SEED, 1,
				5));

	}
}
