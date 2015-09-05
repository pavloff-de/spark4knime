package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "Union" Node. Creates a new RDD that
 * contains the union of two RDD's
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class UnionNodeFactory extends NodeFactory<UnionNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public UnionNodeModel createNodeModel() {
		return new UnionNodeModel();
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
	public NodeView<UnionNodeModel> createNodeView(final int viewIndex,
			final UnionNodeModel nodeModel) {
		return new UnionNodeView(nodeModel);
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
		return new UnionNodeDialog();
	}

}
