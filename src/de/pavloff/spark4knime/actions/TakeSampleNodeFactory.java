package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "TakeSample" Node. Returns a sample of RDD
 * as list
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class TakeSampleNodeFactory extends NodeFactory<TakeSampleNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TakeSampleNodeModel createNodeModel() {
		return new TakeSampleNodeModel();
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
	public NodeView<TakeSampleNodeModel> createNodeView(final int viewIndex,
			final TakeSampleNodeModel nodeModel) {
		return new TakeSampleNodeView(nodeModel);
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
		return new TakeSampleNodeDialog();
	}

}
