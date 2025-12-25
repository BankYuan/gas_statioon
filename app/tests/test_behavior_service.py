import pytest
from unittest.mock import AsyncMock

from app.service.behavior_service import BehaviorService, ActionResult, SegmentResult


@pytest.mark.asyncio
async def test_ok_event_single_segment(tmp_path):
    svc = BehaviorService(redis_client=None)
    video_file = tmp_path / "test.mp4"
    video_file.write_text("dummy")
    seg = SegmentResult(
        object_name="test.mp4",
        clip_time="2025-01-01-00-00-00-000",
        actions=[
            ActionResult(
                nozzle_no=None,
                license_plate_number=None,
                start_time=0,
                end_time=1,
                label_id=1,
                label_name="提油枪",
                classify_score=0.9,
                iou_score=0.9,
            ),
            ActionResult(
                nozzle_no=None,
                license_plate_number=None,
                start_time=2,
                end_time=3,
                label_id=2,
                label_name="挂油枪",
                classify_score=0.9,
                iou_score=0.9,
            ),
        ],
    )
    svc._call_recognition = AsyncMock(return_value=(seg.actions, {}))
    result = await svc.process_clip(str(video_file), clip_time=seg.clip_time, object_name=seg.object_name, camera_id="camera_1", service_name="svc")
    assert result["complete"] is True
    assert result["reason"] == "ok"
    assert result["segments"][0]["object_name"] == "test.mp4"


@pytest.mark.asyncio
async def test_lift_only_timeout(tmp_path):
    svc = BehaviorService(redis_client=None)
    svc.max_segments = 1
    video1 = tmp_path / "s1.mp4"
    video2 = tmp_path / "s2.mp4"
    video1.write_text("dummy")
    video2.write_text("dummy")
    seg1 = SegmentResult(
        object_name="s1.mp4",
        clip_time="2025-01-01-00-00-00-000",
        actions=[
            ActionResult(
                nozzle_no=None,
                license_plate_number=None,
                start_time=0,
                end_time=1,
                label_id=1,
                label_name="提油枪",
                classify_score=0.9,
                iou_score=0.9,
            ),
        ],
    )
    seg2 = SegmentResult(
        object_name="s2.mp4",
        clip_time="2025-01-01-00-01-00-000",
        actions=[],
    )
    svc._call_recognition = AsyncMock(side_effect=[(seg1.actions, {}), (seg2.actions, {})])
    result1 = await svc.process_clip(str(video1), clip_time=seg1.clip_time, object_name=seg1.object_name, camera_id="camera_1", service_name="svc")
    assert result1["complete"] is False
    # 第二段无挂枪，超段触发 lift_only
    result2 = await svc.process_clip(str(video2), clip_time=seg2.clip_time, object_name=seg2.object_name, camera_id="camera_1", service_name="svc")
    assert result2["complete"] is True
    assert result2["reason"] in ("lift_only", "max_segments_reached")


@pytest.mark.asyncio
async def test_hang_only_requires_backtrack_full(tmp_path):
    svc = BehaviorService(redis_client=None)
    svc.hang_only_backtrack = 2
    video1 = tmp_path / "s1.mp4"
    video2 = tmp_path / "s2.mp4"
    video1.write_text("dummy")
    video2.write_text("dummy")
    # 只有挂枪一段，因 backtrack 不足不会触发
    seg = SegmentResult(
        object_name="s1.mp4",
        clip_time="2025-01-01-00-00-00-000",
        actions=[
            ActionResult(
                nozzle_no=None,
                license_plate_number=None,
                start_time=0,
                end_time=1,
                label_id=2,
                label_name="挂油枪",
                classify_score=0.9,
                iou_score=0.9,
            ),
        ],
    )
    seg2 = SegmentResult(
        object_name="s2.mp4",
        clip_time="2025-01-01-00-01-00-000",
        actions=[
            ActionResult(
                nozzle_no=None,
                license_plate_number=None,
                start_time=2,
                end_time=3,
                label_id=2,
                label_name="挂油枪",
                classify_score=0.9,
                iou_score=0.9,
            ),
        ],
    )
    svc._call_recognition = AsyncMock(side_effect=[(seg.actions, {}), (seg2.actions, {})])
    result = await svc.process_clip(str(video1), clip_time=seg.clip_time, object_name=seg.object_name, camera_id="camera_2", service_name="svc")
    assert result["complete"] is False
    # 满足 backtrack 段数且无提枪，下一段挂枪触发
    result2 = await svc.process_clip(str(video2), clip_time=seg2.clip_time, object_name=seg2.object_name, camera_id="camera_2", service_name="svc")
    assert result2["complete"] is True
    assert result2["reason"] == "hang_only"
