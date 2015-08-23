package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeView;

import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * <code>NodeView</code> for the "GroupByKey" Node. When called on a dataset of
 * (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs
 * 
 * @author Oleg Pavlov
 */
public class GroupByKeyNodeView extends NodeView<GroupByKeyNodeModel> {

	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link GroupByKeyNodeModel})
	 */
	protected GroupByKeyNodeView(final GroupByKeyNodeModel nodeModel) {
		super(nodeModel);

		// TODO instantiate the components of the view here.

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {

		// TODO retrieve the new model from your nodemodel and
		// update the view.
		GroupByKeyNodeModel nodeModel = (GroupByKeyNodeModel) getNodeModel();
		assert nodeModel != null;

		// be aware of a possibly not executed nodeModel! The data you retrieve
		// from your nodemodel could be null, emtpy, or invalid in any kind.

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onClose() {

		// TODO things to do when closing the view
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onOpen() {

		// things to do when opening the view
		GroupByKeyNodeModel nodeModel = (GroupByKeyNodeModel) getNodeModel();
		assert nodeModel != null;
		RddViewer view = nodeModel.getRddViewer();
		assert (view != null);
		setComponent(view.getTableView());
	}

}
