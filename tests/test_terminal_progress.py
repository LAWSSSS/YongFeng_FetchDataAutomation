import pytest
from app.presentation import progress_view


@pytest.mark.parametrize('state',['complete','partial_complete','no_target','uncuttable','cleaned','superseded','cancelled','excluded'])
def test_terminal_never_reuses_old_progress(state):
    row=progress_view({'state':state,'progress':100,'analysisTotal':20,'analysisDone':20})
    assert row['terminal'] and row['displayProgress'] is None and row['progress'] is None


def test_only_real_counts_display_progress():
    assert progress_view({'state':'analyzing'})['displayProgress'] is None
    row=progress_view({'state':'analyzing','analysisTotal':20,'analysisDone':5})
    assert not row['terminal'] and row['displayProgress']['done']==5
    assert not progress_view({'state':'platform_processing','progress':100})['terminal']
