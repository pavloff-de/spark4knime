package de.pavloff.spark4knime.input;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "TableToRDD" Node. Read a table from
 * previous node as input and parallelize data by rows.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class TableToRDDNodeFactory extends NodeFactory<TableToRDDNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TableToRDDNodeModel createNodeModel() {
		return new TableToRDDNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		// take and visualize first entries of RDD
		return 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<TableToRDDNodeModel> createNodeView(final int viewIndex,
			final TableToRDDNodeModel nodeModel) {
		return new TableToRDDNodeView(nodeModel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new TableToRDDNodeDialog();
	}

}
