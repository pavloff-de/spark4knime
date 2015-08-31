package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Count" Node. Count elements of RDD
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class CountNodeFactory extends NodeFactory<CountNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CountNodeModel createNodeModel() {
		return new CountNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		return 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<CountNodeModel> createNodeView(final int viewIndex,
			final CountNodeModel nodeModel) {
		return new CountNodeView(nodeModel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new CountNodeDialog();
	}

}
