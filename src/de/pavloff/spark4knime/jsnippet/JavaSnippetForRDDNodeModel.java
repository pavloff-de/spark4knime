/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   24.11.2011 (hofer): created
 */
package de.pavloff.spark4knime.jsnippet;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.FlowVariable.Type;

import de.pavloff.spark4knime.TableCellUtils;
import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * The node model of the java snippet node.
 * 
 * @author Heiko Hofer
 * @author Oleg Pavlov, University of Heidelberg
 */
public class JavaSnippetForRDDNodeModel extends NodeModel {
	private JavaSnippetSettings m_settings;
	private JavaSnippet m_snippet;
	private static final NodeLogger LOGGER = NodeLogger
			.getLogger("Java Snippet");

	// RDD viewer instance
	private RddViewer rddViewer;

	/**
	 * Create a new instance.
	 */
	public JavaSnippetForRDDNodeModel() {
		super(1, 1);
		m_settings = new JavaSnippetSettings();
		m_snippet = new JavaSnippet();
		m_snippet.attachLogger(LOGGER);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
			throws InvalidSettingsException {
		m_snippet.setSettings(m_settings);
		FlowVariableRepository flowVarRepository = new FlowVariableRepository(
				getAvailableInputFlowVariables());
		ValidationReport report = m_snippet.validateSettings(inSpecs[0],
				flowVarRepository);
		if (report.hasWarnings()) {
			setWarningMessage(StringUtils.join(report.getWarnings(), "\n"));
		}
		if (report.hasErrors()) {
			throw new InvalidSettingsException(StringUtils.join(
					report.getErrors(), "\n"));
		}

		DataTableSpec outSpec = m_snippet.configure(inSpecs[0],
				flowVarRepository);
		for (FlowVariable flowVar : flowVarRepository.getModified()) {
			if (flowVar.getType().equals(Type.INTEGER)) {
				pushFlowVariableInt(flowVar.getName(), flowVar.getIntValue());
			} else if (flowVar.getType().equals(Type.DOUBLE)) {
				pushFlowVariableDouble(flowVar.getName(),
						flowVar.getDoubleValue());
			} else {
				pushFlowVariableString(flowVar.getName(),
						flowVar.getStringValue());
			}
		}
		return new DataTableSpec[] { outSpec };
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
			final ExecutionContext exec) throws Exception {
		m_snippet.setSettings(m_settings);
		FlowVariableRepository flowVarRepo = new FlowVariableRepository(
				getAvailableInputFlowVariables());
		BufferedDataTable output = m_snippet.execute(inData[0], flowVarRepo,
				exec);
		for (FlowVariable var : flowVarRepo.getModified()) {
			Type type = var.getType();
			if (type.equals(Type.INTEGER)) {
				pushFlowVariableInt(var.getName(), var.getIntValue());
			} else if (type.equals(Type.DOUBLE)) {
				pushFlowVariableDouble(var.getName(), var.getDoubleValue());
			} else { // case: type.equals(Type.STRING)
				pushFlowVariableString(var.getName(), var.getStringValue());
			}
		}

		// check type of RDD output table
		BufferedDataTable rddOut = output;
		if (TableCellUtils.isRDDTable(output)) {
			if (TableCellUtils.isPairRDD(output)) {
				rddOut = TableCellUtils.setRDD(exec,
						((JavaPairRDD) TableCellUtils.getRDD(output)), true);

			} else {
				rddOut = TableCellUtils.setRDD(exec,
						((JavaRDD) TableCellUtils.getRDD(output)), false);
			}
		}

		// update RDD viewer
		rddViewer = new RddViewer(rddOut, exec);

		return new BufferedDataTable[] { rddOut };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		// save user settings to the config object.

		m_settings.saveSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		// check if the settings could be applied to our model e.g. if the count
		// is in a certain range (which is ensured by the SettingsModel). Do not
		// actually set any values of any member variables.

		JavaSnippetSettings s = new JavaSnippetSettings();
		s.loadSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		// load (valid) settings from the config object. It can be safely
		// assumed that the settings are valided by the method below.

		m_settings.loadSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// Code executed on reset. Models build during execute are cleared here.
		// Also data handled in load/saveInternals will be erased here.

		rddViewer = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File nodeInternDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {
		// load internal data. Everything handed to output ports is loaded
		// automatically (data returned by the execute method, models loaded in
		// loadModelContent, and user settings set through loadSettingsFrom - is
		// all taken care of). Load here only the other internals that need to
		// be restored (e.g. data used by the views).
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File nodeInternDir,
			final ExecutionMonitor exec) throws IOException,
			CanceledExecutionException {
		// save internal models. Everything written to output ports is saved
		// automatically (data returned by the execute method, models saved in
		// the saveModelContent, and user settings saved through saveSettingsTo
		// - is all taken care of). Save here only the other internals that need
		// to be preserved (e.g. data used by the views).
	}

	/**
	 * @return <code>RddViewer</code> of the model
	 */
	public RddViewer getRddViewer() {
		return rddViewer;
	}
}