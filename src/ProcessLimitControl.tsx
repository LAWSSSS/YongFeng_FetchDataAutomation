import './process-management.css';

type Props = {
  label: string; value: number; dirty: boolean; disabled?: boolean; saving?: boolean;
  buttonLabel: string; onChange: (value: number) => void; onSave: () => void;
};

export default function ProcessLimitControl({label, value, dirty, disabled = false, saving = false,
  buttonLabel, onChange, onSave}: Props) {
  const canSave = !disabled && !saving && dirty && Number.isInteger(value) && value >= 1 && value <= 8;
  return <div className="slice-process-control">
    <label>{label}<input type="number" min={1} max={8} step={1} value={value}
      disabled={disabled || saving} onChange={event => onChange(Number(event.target.value))}
      onKeyDown={event => {if (event.key === 'Enter' && canSave) {event.preventDefault();onSave();}}} /></label>
    <button type="button" disabled={!canSave} onClick={onSave}>{saving ? '保存中…' : buttonLabel}</button>
  </div>;
}
